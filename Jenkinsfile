#!groovy

stage("Unit tests") {
    node('') {
        checkout scm

        wrap([$class: 'AnsiColorBuildWrapper']) {
            sh '''
            SPARK_CONTAINER="spark-stat-analyser_run_$BUILD_NUMBER"

            docker-compose -f docker-composer.test.yml build
            docker-compose -f docker-composer.test.yml run --name $SPARK_CONTAINER spark-stat-analyser
            docker cp $SPARK_CONTAINER:/srv/spark-stat-analyzer/junit.xml .
            docker-compose -f docker-composer.test.yml down --remove-orphans
            '''
            junit 'junit.xml'
        }
    }
}

