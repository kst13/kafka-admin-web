// kafka-admin 배포 파이프라인
// 흐름: GitLab 체크아웃(Pipeline from SCM) → 이미지 빌드 → 10.10.10.19 전송 → compose 기동
//
// 사전 준비 (1회):
//  - Jenkins: docker 실행 가능한 에이전트, SSH 키 credential 등록 (아래 SSH_CRED_ID)
//  - 서버(10.10.10.19): /home/ow/kafka-admin/ 에 .env 와 secrets/truststore.jks 배치
//    (.env 는 deploy/.env.example 기준, truststore 는 /home/ow/kafka/secret/ 것 재사용 가능)
//  - 기존 Kafka docker-compose 와는 디렉터리·프로젝트가 분리되어 서로 영향 없음
pipeline {
  agent any

  options {
    disableConcurrentBuilds()
    timestamps()
  }

  environment {
    DEPLOY_HOST = 'ow@10.10.10.19'
    DEPLOY_DIR  = '/home/ow/kafka-admin'
    SSH_CRED_ID = 'jenkins-ow' // Jenkins 에 등록한 SSH private key credential ID
  }

  stages {
    stage('Build images') {
      steps {
        sh 'docker build -t kafka-admin-was:${BUILD_NUMBER} was'
        sh 'docker build -t kafka-admin-web:${BUILD_NUMBER} web'
      }
    }

    stage('Ship & Deploy') {
      steps {
        sshagent(credentials: [env.SSH_CRED_ID]) {
          sh '''
            docker save kafka-admin-was:${BUILD_NUMBER} kafka-admin-web:${BUILD_NUMBER} | gzip > images.tgz
            ssh -o StrictHostKeyChecking=accept-new ${DEPLOY_HOST} "mkdir -p ${DEPLOY_DIR}"
            scp images.tgz deploy/docker-compose.yml ${DEPLOY_HOST}:${DEPLOY_DIR}/
            ssh ${DEPLOY_HOST} "cd ${DEPLOY_DIR} \
              && docker load < images.tgz \
              && TAG=${BUILD_NUMBER} docker compose up -d \
              && rm -f images.tgz \
              && docker image prune -f"
          '''
        }
      }
    }

    stage('Smoke check') {
      steps {
        sshagent(credentials: [env.SSH_CRED_ID]) {
          // web(80) 이 응답하면 성공으로 본다 (로그인 페이지). 기동 대기 최대 60초.
          sh '''
            ssh ${DEPLOY_HOST} '
              for i in $(seq 1 30); do
                code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost/ || true)
                [ "$code" = "200" ] && echo "web up (200)" && exit 0
                sleep 2
              done
              echo "web 미응답 — docker compose logs 확인 필요" && exit 1
            '
          '''
        }
      }
    }
  }

  post {
    success {
      echo "배포 완료: http://10.10.10.19/ (이미지 TAG=${BUILD_NUMBER})"
    }
    always {
      sh 'rm -f images.tgz'
      // Jenkins 에이전트의 빌드 이미지 정리 (직전 태그는 재배포 대비 남겨두려면 이 줄 제거)
      sh 'docker rmi kafka-admin-was:${BUILD_NUMBER} kafka-admin-web:${BUILD_NUMBER} || true'
    }
  }
}
