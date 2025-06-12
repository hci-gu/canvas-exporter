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
          const next = downloadQueue.shift()
          activeDownloads++
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

async function downloadWithResume(url, filePath, retries = 5) {
  const alreadyHave = fs.existsSync(filePath) ? fs.statSync(filePath).size : 0
  const headers = {
    Authorization: `Bearer ${CANVAS_API_TOKEN}`,
    Range: `bytes=${alreadyHave}-`,
  }

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await axios.get(url, {
        responseType: 'stream',
        headers,
        httpsAgent,
        timeout: 0,
        maxRedirects: 5,
      })

      if (![200, 206].includes(res.status)) {
        throw new Error(`Unexpected status ${res.status}`)
      }

      await pipeline(res.data, fs.createWriteStream(filePath, { flags: 'a' }))

      return
    } catch (err) {
      if (attempt === retries) throw err
      const delay = 2 ** attempt * 1_000
      console.warn(`retrying in ${delay / 1000}s - ${err.message}`)
      await wait(delay)
    }
  }
}

const folderForCourse = (course) => {
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

const getActiveCourseExport = async (courseId) => {
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
    const activeExports = exports.filter(
      (exportItem) =>
        exportItem.workflow_state === 'exported' &&
        exportItem.attachment &&
        new Date(exportItem.created_at) >= EXPORT_AFTER_THIS_DATE
    )
    console.log(
      `Found ${activeExports.length} active exports for course ID ${courseId}`
    )
    if (activeExports.length === 0) {
      return null
    }
    // newest first
    activeExports.sort(
      (a, b) => new Date(b.created_at) - new Date(a.created_at)
    )
    return activeExports[0]
  } catch (error) {
    console.error(`Error fetching exports for course ID ${courseId}:`, error)
    return null
  }
}

const createCourseExport = async (courseId) => {
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

const downloadFilesFromCourse = async (course) => {
  const activeExport = await getActiveCourseExport(course.id)

  if (!activeExport) {
    console.log(
      `No active export found for course ID ${course.id}. Creating a new export...`
    )
    await createCourseExport(course.id)
    return null
  }

  if (activeExport.workflow_state === 'exported' && activeExport.attachment) {
    const fileUrl = activeExport.attachment.url
    const fileName = activeExport.attachment.filename
    const folder = folderForCourse(course)
    const filePath = `${folder}/${fileName}`

    // Use the download queue
    return enqueueDownloadTask(() => downloadWithResume(fileUrl, filePath))
  }

  return null
}

const checkIfCourseFileExported = async (course) => {
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
      const downloadPromise = await downloadFilesFromCourse(course)
      if (downloadPromise) {
        // Already exported and enqueued for download
        downloadPromises.push(downloadPromise)
      } else {
        // Export is being created, check again later
        stillPending.push(course)
      }
      await wait(250)
    }

    // Wait for all downloads that have been enqueued in this cycle
    if (downloadPromises.length) {
      await Promise.all(downloadPromises)
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

const getAllCourses = async (
  url = `${BASE_URL}/accounts/49/courses?per_page=100`,
  allCourses = []
) => {
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

  const nextPage = response.headers.link
    ? response.headers.link.match(/<([^>]+)>;\s*rel="next"/)
    : null

  allCourses.push(...courses)
  fs.writeFileSync(
    `${EXPORT_FOLDER}/courses.json`,
    JSON.stringify(allCourses, null, 2)
  )

  console.log(`Fetched ${courses.length} courses. Total: ${allCourses.length}`)

  return nextPage ? getAllCourses(nextPage[1], allCourses) : allCourses
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
