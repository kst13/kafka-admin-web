import { fileURLToPath, URL } from 'node:url'

import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import vueDevTools from 'vite-plugin-vue-devtools'

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    vue(),
    vueDevTools(),
  ],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  server: {
    // 8080이 다른 프로세스에 점유된 환경에서는 API_PROXY_TARGET으로 대상 변경
    proxy: { '/api': process.env.API_PROXY_TARGET || 'http://localhost:8080' },
  },
})
