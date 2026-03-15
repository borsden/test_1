# Third-party artifacts

- `jdk-21.0.2+13/` — вендорный JDK, используемый для запуска SBE codegen. Скачайте
  архив, например, отсюда: https://download.java.net/java/GA/jdk21/13/GPL/openjdk-21.0.2_linux-x64_bin.tar.gz
  и распакуйте в `third_party/jdk-21.0.2+13/`.
- `sbe-all-1.38.0.jar` — jar-файл c официальным генератором Real Logic SBE.

Jar SBE не включён в репозиторий. Скачайте его, например, по адресу:
https://repo1.maven.org/maven2/uk/co/real-logic/sbe-all/1.38.0/sbe-all-1.38.0.jar
и положите по пути `third_party/sbe-all-1.38.0.jar`.
