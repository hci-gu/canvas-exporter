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

const wait = (ms) => {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

const httpsAgent = new https.Agent({ keepAlive: true, maxSockets: 8 })

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
      await new Promise((r) => setTimeout(r, delay))
    }
  }
}

const folderForCourse = (course) => {
  const startDateString = course.start_at || course.created_at
  const startDate = new Date(startDateString)

  const startYear = startDate.getFullYear()

  // Sanitize course name by replacing unsafe characters
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
      console.log(`No active exports found for course ID ${courseId}`)
      return null
    }
    // Sort by created_at in descending order to get the most recent export
    activeExports.sort(
      (a, b) => new Date(b.created_at) - new Date(a.created_at)
    )
    const activeExport = activeExports[0]

    if (activeExport) {
      console.log(
        `Active export found for course ID ${courseId}:`,
        activeExport
      )
      return activeExport
    } else {
      console.log(`No active export found for course ID ${courseId}`)
      return null
    }
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
    console.log(`Export created for course ID ${courseId}:`, response.data)
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
    return false
  }

  if (activeExport.workflow_state == 'exported' && activeExport.attachment) {
    const fileUrl = activeExport.attachment.url
    const fileName = activeExport.attachment.filename
    const folder = folderForCourse(course)
    const filePath = `${folder}/${fileName}`

    return downloadWithResume(fileUrl, filePath)
  }

  return false
}

const checkIfCourseFileExported = async (course) => {
  const folder = folderForCourse(course)

  // check if any *.imscc file exists in the folder
  const files = fs.readdirSync(folder)
  for (const file of files) {
    if (file.endsWith('.imscc')) {
      return true
    }
  }

  return false
}

async function checkStatusAndDownload(courses) {
  let allDone = true

  for (const course of courses) {
    if (await checkIfCourseFileExported(course)) {
      console.log(`Course "${course.name}" already exported. Skipping...`)
      continue
    }
    allDone = false
    console.log(`Processing course: ${course.name} (${course.id})`)

    await downloadFilesFromCourse(course)
    await wait(250)
  }

  if (allDone) {
    console.log('All courses have been processed. Exiting...')
    return
  }
  await wait(1000)
  checkStatusAndDownload(courses)
}

const getAllCourses = async (
  url = `${BASE_URL}/accounts/49/courses?per_page=100`,
  allCourses = []
) => {
  if (allCourses.length === 0) {
    const cachedCourses = fs.existsSync(`${EXPORT_FOLDER}/courses.json`)
    if (cachedCourses) {
      const cachedData = fs.readFileSync(
        `${EXPORT_FOLDER}/courses.json`,
        'utf-8'
      )
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

  return getAllCourses(nextPage ? nextPage[1] : null, allCourses)
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
    // console.log(parsedCsv)
    const matchedCourses = courses.filter((course) => {
      const courseCode = course.name.split(' ')[0]
      // does courseCode starat with TIA or TIG
      // if (courseCode.startsWith('TIA') || courseCode.startsWith('TIG')) {
      //   console.log(`Checking course code: ${courseCode}`)
      // }
      return parsedCsv.some((row) => row.kod === courseCode)
    })
    console.log(matchedCourses.length)
    const coursesToExport = matchedCourses.filter((course) => {
      const startDate = new Date(course.start_at || course.created_at)
      // is after 1st January 2021
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
