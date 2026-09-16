/**
 * 待上传的文件与模拟需求暂存。
 * 首页点击启动引擎后立即跳转，真正的 API 调用在 Process 页面发起。
 * 内容同时落 IndexedDB：刷新页面后依然可以继续走完流程。
 */
import { reactive } from 'vue'

const DB_NAME = 'mirofish'
const DB_VERSION = 1
const STORE_NAME = 'pending_upload'
const RECORD_KEY = 'pending'

const state = reactive({
  files: [],
  simulationRequirement: '',
  isPending: false
})

let dbPromise = null

function openDb() {
  if (typeof indexedDB === 'undefined') return Promise.resolve(null)
  if (!dbPromise) {
    dbPromise = new Promise((resolve) => {
      const request = indexedDB.open(DB_NAME, DB_VERSION)
      request.onupgradeneeded = () => {
        const db = request.result
        if (!db.objectStoreNames.contains(STORE_NAME)) {
          db.createObjectStore(STORE_NAME)
        }
      }
      request.onsuccess = () => resolve(request.result)
      request.onerror = () => resolve(null)
    })
  }
  return dbPromise
}

async function runStore(mode, action) {
  const db = await openDb()
  if (!db) return null
  return new Promise((resolve) => {
    const tx = db.transaction(STORE_NAME, mode)
    const request = action(tx.objectStore(STORE_NAME))
    request.onsuccess = () => resolve(request.result === undefined ? null : request.result)
    request.onerror = () => resolve(null)
    tx.onabort = () => resolve(null)
  })
}

export async function setPendingUpload(files, requirement) {
  state.files = files
  state.simulationRequirement = requirement
  state.isPending = true
  const payload = {
    simulationRequirement: requirement,
    files: files.map((file) => ({
      name: file.name,
      type: file.type,
      lastModified: file.lastModified,
      blob: file.slice(0, file.size, file.type)
    }))
  }
  await runStore('readwrite', (store) => store.put(payload, RECORD_KEY))
}

export function getPendingUpload() {
  return {
    files: state.files,
    simulationRequirement: state.simulationRequirement,
    isPending: state.isPending
  }
}

export async function hydratePendingUpload() {
  if (state.isPending) return
  const payload = await runStore('readonly', (store) => store.get(RECORD_KEY))
  if (!payload || !payload.files || !payload.files.length) return
  state.files = payload.files.map(
    (item) => new File([item.blob], item.name, { type: item.type, lastModified: item.lastModified })
  )
  state.simulationRequirement = payload.simulationRequirement || ''
  state.isPending = true
}

export async function clearPendingUpload() {
  state.files = []
  state.simulationRequirement = ''
  state.isPending = false
  await runStore('readwrite', (store) => store.delete(RECORD_KEY))
}

export default state
