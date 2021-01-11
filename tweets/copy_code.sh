rm ../quickstart/src/main/java/piprescott/*
rm -rf ../quickstart/src/test
cp ./java/* ../quickstart/src/main/java/piprescott/
cp twitter4j.properties ../quickstart/twitter4j.properties
cp working_pom.xml ../quickstart/pom.xml
cp package_and_run.sh ../quickstart/package_and_run.sh

