#!groovy

stage("Unit tests") {
    node('') {

        checkout scm

        wrap([$class: 'AnsiColorBuildWrapper']) {
            sh '''
            USER_ID=$(id -u) docker-compose -f docker-composer.test.yml up -d
            docker wait $(docker-compose -f docker-composer.test.yml ps -q spark-stat-analyser)
            docker-compose -f docker-composer.test.yml down
            '''
            junit 'junit.xml'
        }
    }
}
