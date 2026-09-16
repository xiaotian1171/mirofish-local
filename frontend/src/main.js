import { createApp } from 'vue'
import App from './App.vue'
import router from './router'
import i18n from './i18n'
import { hydratePendingUpload } from './store/pendingUpload'

const app = createApp(App)

app.use(router)
app.use(i18n)

// 先恢复未完成的上传（刷新后继续），再挂载应用
hydratePendingUpload().finally(() => app.mount('#app'))
