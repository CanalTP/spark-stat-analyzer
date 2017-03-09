#!groovy

stage("Unit tests") {
    node('') {

        checkout scm

        wrap([$class: 'AnsiColorBuildWrapper']) {
            sh '''
            USER_ID=$(id -u) docker-compose -f docker-composer.test.yml up -d
            SPARK_CONTAINER=$(docker-compose -f docker-composer.test.yml ps -q spark-stat-analyser)
            docker wait $SPARK_CONTAINER
            docker cp $SPARK_CONTAINER:/srv/spark-stat-analyzer/junit.xml .
            docker-compose -f docker-composer.test.yml down
            '''
            junit 'junit.xml'
        }
    }
}
