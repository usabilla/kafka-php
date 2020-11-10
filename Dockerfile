FROM 644152709166.dkr.ecr.eu-west-1.amazonaws.com/usabilla/dev/dockerhub-mirror/php:7.1-alpine

WORKDIR /opt/kafka-php

CMD ["./vendor/bin/phpunit", "--testsuite", "functional"]
