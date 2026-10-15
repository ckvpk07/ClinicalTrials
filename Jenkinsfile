
pipeline {
    agent any

    options {
        timestamps()
        disableConcurrentBuilds()
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Verify Repository') {
            steps {
                echo 'Jenkins successfully checked out the feature branch.'
                sh 'git branch --show-current'
                sh 'ls -la'
            }
        }
    }
}
