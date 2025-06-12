import { config } from 'dotenv'
config()
import { pipeline } from 'stream/promises'
import axios from 'axios'
import fs from 'fs'
import { parse } from 'csv-parse/sync'
import https from 'https'

const EXPORT_AFTER_THIS_DATE = new Date('2025-06-08')
const EXPORT_FOLDER = '/Volumes/Backup/canvas'

const { CANVAS_API_TOKEN, CANVAS_DOMAIN } = process.env
const BASE_URL = `https://${CANVAS_DOMAIN}/api/v1`

const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

/* ------------------------------ Download queue ------------------------------ */

const MAX_CONCURRENT_DOWNLOADS = 10
let activeDownloads = 0
const downloadQueue = []

/**
 * Schedule a download task so that no more than MAX_CONCURRENT_DOWNLOADS are
 * running at the same time.
 *
 * @param {() => Promise<void>} taskFn  Function that performs the download
 * @returns {Promise<void>}             Resolves when the download finishes
 */
function enqueueDownloadTask(taskFn) {
  return new Promise((resolve, reject) => {
    const execute = async () => {
      try {
        await taskFn()
        resolve()
      } catch (err) {
        reject(err)
      } finally {
        activeDownloads--
        if (downloadQueue.length > 0) {
          activeDownloads++
          const next = downloadQueue.shift()
          next()
        }
      }
    }

    if (activeDownloads < MAX_CONCURRENT_DOWNLOADS) {
      activeDownloads++
      execute()
    } else {
      downloadQueue.push(execute)
    }
  })
}

/* --------------------------------------------------------------------------- */

const httpsAgent = new https.Agent({
  keepAlive: true,
  maxSockets: MAX_CONCURRENT_DOWNLOADS,
})

/**
 * Downloads a file with HTTP range‑resume support.
 * Handles server responses 200, 206 (partial) and 416 (finished).
 */
async function downloadWithResume(url, filePath, retries = 5) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    // Re‑compute current file size before every attempt
    const alreadyHave = fs.existsSync(filePath) ? fs.statSync(filePath).size : 0

    const headers = {
      Authorization: `Bearer ${CANVAS_API_TOKEN}`,
    }
    if (alreadyHave > 0) {
      headers.Range = `bytes=${alreadyHave}-`
    }

    try {
      const res = await axios.get(url, {
        responseType: 'stream',
        headers,
        httpsAgent,
        timeout: 0,
        maxRedirects: 5,
        validateStatus: (status) => [200, 206, 416].includes(status),
      })

      // 416 means we already have the full file
      if (res.status === 416) {
        return
      }

      await pipeline(res.data, fs.createWriteStream(filePath, { flags: 'a' }))
      return // success
    } catch (err) {
      if (attempt === retries) {
        throw err
      }
      const delay = Math.min(2 ** attempt * 1_000, 30_000)
      console.warn(
        `Retry ${attempt + 1}/${retries} in ${delay / 1000}s - ${err.message}`
      )
      await wait(delay)
    }
  }
}

function folderForCourse(course) {
  const startDateString = course.start_at || course.created_at
  const startDate = new Date(startDateString)
  const startYear = startDate.getFullYear()
  const sanitizedCourseName = course.name.replace(/[\/\\:*?"<>|]/g, '_')
  const folder = `${EXPORT_FOLDER}/${startYear}/${sanitizedCourseName}`
  if (!fs.existsSync(folder)) {
    fs.mkdirSync(folder, { recursive: true })
  }
  return folder
}

async function getActiveCourseExport(courseId) {
  try {
    const response = await axios.get(
      `${BASE_URL}/courses/${courseId}/content_exports?per_page=100`,
      {
        headers: {
          Authorization: `Bearer ${CANVAS_API_TOKEN}`,
        },
      }
    )
    const exports = response.data
    const activeExports = exports
      .filter(
        (exportItem) =>
          exportItem.workflow_state === 'exported' &&
          exportItem.attachment &&
          new Date(exportItem.created_at) >= EXPORT_AFTER_THIS_DATE
      )
      .sort((a, b) => new Date(b.created_at) - new Date(a.created_at)) // newest first

    console.log(
      `Found ${activeExports.length} active exports for course ID ${courseId}`
    )

    return activeExports[0] ?? null
  } catch (error) {
    console.error(`Error fetching exports for course ID ${courseId}:`, error)
    return null
  }
}

async function createCourseExport(courseId) {
  try {
    const response = await axios.post(
      `${BASE_URL}/courses/${courseId}/content_exports`,
      {
        export_type: 'common_cartridge',
        skip_notifications: true,
        include_quiz_questions: false,
      },
      {
        headers: {
          Authorization: `Bearer ${CANVAS_API_TOKEN}`,
        },
      }
    )
    console.log(`Export created for course ID ${courseId}`)
    return response.data
  } catch (error) {
    console.error(`Error creating export for course ID ${courseId}:`, error)
    return null
  }
}

async function downloadFilesFromCourse(course) {
  const activeExport = await getActiveCourseExport(course.id)

  if (!activeExport) {
    console.log(
      `No active export found for course ID ${course.id}. Creating a new export...`
    )
    await createCourseExport(course.id)
    return null // will be picked up in a later poll
  }

  if (activeExport.workflow_state === 'exported' && activeExport.attachment) {
    const { url: fileUrl, filename: fileName } = activeExport.attachment
    const folder = folderForCourse(course)
    const filePath = `${folder}/${fileName}`

    // Use the download queue
    return enqueueDownloadTask(() => downloadWithResume(fileUrl, filePath))
  }

  return null
}

async function checkIfCourseFileExported(course) {
  const folder = folderForCourse(course)
  return (
    fs.existsSync(folder) &&
    fs.readdirSync(folder).some((f) => f.endsWith('.imscc'))
  )
}

async function checkStatusAndDownload(courses) {
  let pendingCourses = courses

  while (true) {
    const downloadPromises = []
    const stillPending = []

    for (const course of pendingCourses) {
      if (await checkIfCourseFileExported(course)) {
        console.log(`Course "${course.name}" already exported. Skipping...`)
        continue
      }

      console.log(`Processing course: ${course.name} (${course.id})`)
      const downloadPromise = downloadFilesFromCourse(course) // ← no await
      if (downloadPromise) {
        downloadPromises.push(downloadPromise)
      } else {
        stillPending.push(course)
      }
      await wait(250)
    }

    // Wait for all downloads that have been enqueued in this cycle
    if (downloadPromises.length) {
      try {
        await Promise.all(downloadPromises)
      } catch (err) {
        console.error('One or more downloads failed:', err)
      }
    }

    // If there are no more pending courses, we're done
    if (stillPending.length === 0) {
      console.log('All courses have been processed. Exiting...')
      return
    }

    // Otherwise, wait before checking again
    pendingCourses = stillPending
    console.log(
      `Waiting for ${pendingCourses.length} export(s) to finish before retrying...`
    )
    await wait(30_000)
  }
}

async function getAllCourses(
  url = `${BASE_URL}/accounts/49/courses?per_page=100`,
  allCourses = []
) {
  // Try read from cache first
  if (allCourses.length === 0) {
    const cachedPath = `${EXPORT_FOLDER}/courses.json`
    if (fs.existsSync(cachedPath)) {
      const cachedData = fs.readFileSync(cachedPath, 'utf-8')
      allCourses = JSON.parse(cachedData)
      console.log(`Loaded ${allCourses.length} courses from cache.`)
      return allCourses
    }
  }

  const response = await axios.get(url, {
    headers: {
      Authorization: `Bearer ${CANVAS_API_TOKEN}`,
    },
  })

  const courses = response.data
  if (courses.length === 0) {
    return allCourses
  }

  // Parse the Link header safely (can contain multiple comma‑separated links)
  let nextUrl = null
  if (response.headers.link) {
    const links = response.headers.link.split(',').map((s) => s.trim())
    for (const l of links) {
      const match = l.match(/<([^>]+)>;\s*rel="next"/)
      if (match) {
        nextUrl = match[1]
        break
      }
    }
  }

  allCourses.push(...courses)
  fs.mkdirSync(EXPORT_FOLDER, { recursive: true })
  fs.writeFileSync(
    `${EXPORT_FOLDER}/courses.json`,
    JSON.stringify(allCourses, null, 2)
  )

  console.log(`Fetched ${courses.length} courses. Total: ${allCourses.length}`)

  return nextUrl ? getAllCourses(nextUrl, allCourses) : allCourses
}

async function main() {
  try {
    const courses = await getAllCourses()
    console.log(`Found ${courses.length} courses.`)

    const csvData = fs.readFileSync('./canvas-courses.csv', 'utf-8')
    const parsedCsv = parse(csvData, {
      columns: true,
      skip_empty_lines: true,
      delimiter: ';',
    })

    const matchedCourses = courses.filter((course) => {
      const courseCode = course.name.split(' ')[0]
      return parsedCsv.some((row) => row.kod === courseCode)
    })

    const coursesToExport = matchedCourses.filter((course) => {
      const startDate = new Date(course.start_at || course.created_at)
      return startDate >= new Date('2021-01-01')
    })

    console.log(`Filtered courses to export: ${coursesToExport.length}`)

    await checkStatusAndDownload(coursesToExport)

    // TODO: unpack and rename files
  } catch (error) {
    if (error.response) {
      console.error('Error response from Canvas API:', error.response.data)
    } else if (error.request) {
      console.error('No response received from Canvas API:', error.request)
    } else {
      console.error('Error setting up request to Canvas API:', error.message)
    }
  }
}

main()
