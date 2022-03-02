function onOpen() {
  SpreadsheetApp
    .getActiveSpreadsheet()
    .addMenu('集計', [
      {name: 'Monthlyを集計', functionName: 'calculateMonthly'},
      {name: 'Dailyを集計', functionName: 'calculateDaily'},
    ]);
}

function zeroFill(number, length) {
  return `${number}`.padStart(length, '0')
}

function isEmpty(value) {
  if (typeof value == 'string') {
    return value.length == 0
  } else if (typeof value == 'number') {
    return value == 0
  } else {
    return true
  }
}

const projectId = PropertiesService.getScriptProperties().getProperty('PROJECT_ID')
const eventName = "user_engagement"
const tableName = PropertiesService.getScriptProperties().getProperty('TABLE_NAME')

/* Monthly */

/**
 * 月別の集計（iOS）を実行する
 */
function calculateMonthly() {
  console.log('start calculateMonthly')
  const sheetName = 'monthly'
  const startRow = startRowForMonthlyIn(sheetName)
  calculateMonthlyFrom(sheetName, startRow)
}

/**
 * Monthlyの開始位置を取得
 * @param {string} sheetName シート名
 * @return {number} 開始位置
 */
function startRowForMonthlyIn(sheetName) {
  const sheet = SpreadsheetApp.getActive().getSheetByName(sheetName)
  let startRow = 2
  let value = "XXX"
  while (value.length != 0) {
    let range = sheet.getRange(`D${startRow}`)
    value = range.getValue()
    if (!isEmpty(value)) {
      startRow += 1
    } else {
      break
    }
  }
  return startRow
}

/**
 * 月別で集計する
 * @param {string} platform iOS or Android
 * @param {number} startRow 開始位置
 */
function calculateMonthlyFrom(platform, startRow) {
  console.log(`platform ${platform} startRow ${startRow}`)
  const sheet = SpreadsheetApp.getActive().getSheetByName(platform)
  const numberOfRows = sheet.getLastRow()
  for (let i = startRow; i <= numberOfRows; i++) {
    const range = sheet.getRange(`A${i}:C${i}`)
    const values = range.getValues()
    if (values.length == 0) { continue; }
    const year = values[0][0]
    const month = values[0][1]
    const users = countMonthlyUser(year, month)
    const setRange = sheet.getRange(`C${i}`)
    setRange.setValue(users)
  }
}

/**
 * 月別のBigQueryを叩く
 * @param {number} year
 * @param {number} month
 */
function countMonthlyUser(year, month) {
  const from = `${zeroFill(year, 4)}${zeroFill(month, 2)}01`
  const toDate = new Date(year, month, 0)
  const to = `${zeroFill(year, 4)}${zeroFill(month, 2)}${zeroFill(toDate.getDate(), 2)}`
  const query = `SELECT
FORMAT_TIMESTAMP("%Y-%m", TIMESTAMP_MICROS(event_timestamp), "Asia/Tokyo") AS date
  , COUNT(distinct if(user_id IS NOT NULL, user_id, user_pseudo_id)) AS count_user
FROM \`${tableName}.events_*\`
WHERE _TABLE_SUFFIX BETWEEN '${from}' AND '${to}'
GROUP BY
  date
`
  const request = {
    query: query,
    useLegacySql: false
  }
  const result = BigQuery.Jobs.query(request, projectId)
  if (!result || !result.rows) {
    return 0
  } else if (result.rows.length == 0) {
    return 0
  } else {
    if (result.rows[0].f.length == 0) {
      return 0
    } else {
      return result.rows[0].f[1].v
    }
  }
}

/* Daily */

/**
 * 日毎で集計する（iOS）
 */
function calculateDaily() {
  console.log('start calculateDaily')
  let sheetName = 'daily'
  const startRow = startRowForDailyIn(sheetName)
  calculateDailyFrom(sheetName, startRow)
}

/**
 * Dailyの開始位置を取得
 * @param {string} sheetName シート名
 * @return {number} 開始位置
 */
function startRowForDailyIn(sheetName) {
  const sheet = SpreadsheetApp.getActive().getSheetByName(sheetName)
  let startRow = 2
  let value = 999
  while (value.length != 0) {
    let range = sheet.getRange(`E${startRow}`)
    value = range.getValue()
    console.log(`startRow ${startRow} value ${value} length ${value.length}`)
    if (!isEmpty(value)) {
      startRow += 1
    } else {
      break;
    }
  }
  return startRow
}

/**
 * 日別で集計する
 * @param {string} sheetName シート名
 * @param {number} startRow 集計開始位置
 */
function calculateDailyFrom(sheetName, startRow) {
  console.log(`sheetName ${sheetName} startRow ${startRow}`)
  const sheet = SpreadsheetApp.getActive().getSheetByName(sheetName)
  const numberOfRows = sheet.getLastRow()
  for (let i = startRow; i <= numberOfRows; i++) {
    const range = sheet.getRange(`A${i}:D${i}`)
    const values = range.getValues()
    if (values.length == 0) { continue; }
    const year = values[0][0]
    const month = values[0][1]
    const day = values[0][2]
    const users = countDailyUser(year, month, day)
    const setRange = sheet.getRange(`D${i}`)
    setRange.setValue(users)
  }
}

/**
 * 日別のBigQueryを叩く
 * @param {number} year
 * @param {number} month
 * @param {number} day
 */
function countDailyUser(year, month, day) {
  console.log(`year ${year} month ${month} day ${day}`);
  const table = `${tableName}.events_${zeroFill(year, 4)}${zeroFill(month, 2)}${zeroFill(day, 2)}`
  const query = `SELECT
  COUNT(distinct if(user_id IS NOT NULL, user_id, user_pseudo_id)) AS count_user
FROM \`${table}\`
WHERE event_name = "${eventName}"
`
  const request = {
    query: query,
    useLegacySql: false
  }
  const result = BigQuery.Jobs.query(request, projectId)
  if (!result || !result.rows) {
    return 0
  } else if (result.rows.length == 0) {
    return 0
  } else {
    if (result.rows[0].f.length == 0) {
      return 0
    } else {
      return result.rows[0].f[0].v
    }
  }
}