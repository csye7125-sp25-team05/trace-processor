pipeline {
    agent any

    environment {
        registry = "mayu007/cyse7125-sp25-05.rocks"
        DOCKER_ID = 'mayu007'
        imageName = "go-app"
    }

    stages {
        stage('Clone Repository') {
            steps {
                git credentialsId: 'github-pat', branch: 'main', url: 'https://github.com/csye7125-sp25-team05/trace-processor.git'
            }
        }
        stage('Release') {
            steps {
                withCredentials([usernamePassword(credentialsId: 'github-pat', usernameVariable: 'GITHUB_USERNAME', passwordVariable: 'GITHUB_TOKEN')]) {
                    script {
                        sh "npx semantic-release"
                    }
                }
            }
        }

        stage('Build and Push Docker Image') {
            steps {
                script {
                    def latestTag = sh(script: 'git describe --abbrev=0 --tags', returnStdout: true).trim()
                    echo "Using release tag: ${latestTag}"

                    withCredentials([string(credentialsId: 'docker-pat', variable: 'DOCKER_PASSWORD')]) {
                        sh 'echo $DOCKER_PASSWORD | docker login -u $DOCKER_ID --password-stdin'
                        sh 'docker buildx rm newbuilderx || true'
                        sh 'docker buildx create --use --name newbuilderx --driver docker-container'
                        sh "docker buildx build --file Dockerfile --platform linux/amd64,linux/arm64 -t ${registry}:${latestTag} --push ."
                        sh 'docker buildx rm newbuilderx'
                    }
                }
            }
        }
    }
}