call mvn clean package -DskipTests
call mvn install:install-file -Dfile=./target/bidb-1.0.0.jar -DgroupId=org.digga.bidb -DartifactId=bidb -Dversion=1.0.0 -Dpackaging=jar

