FROM ubuntu:20.04

# 환경 변수 설정하여 tzdata 입력 대기 방지
ENV DEBIAN_FRONTEND=noninteractive

# Ubuntu 패키지 설치 (입력 없이 진행되도록 설정)
RUN apt update && \
    apt install -y wget openjdk-8-jdk lsb-release gnupg curl python3 git sudo tzdata && \
    ln -fs /usr/share/zoneinfo/Asia/Seoul /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata

# 2️⃣ Impala 바이너리 다운로드 및 압축 해제 (올바른 경로 사용)
RUN wget https://archive.apache.org/dist/impala/4.4.1/apache-impala-4.4.1.tar.gz
RUN tar -xvzf apache-impala-4.4.1.tar.gz -C /opt
RUN mv /opt/apache-impala-4.4.1 /opt/impala

# 3️⃣ 환경 변수 설정
ENV PATH="/opt/impala/bin:${PATH}"

# 4️⃣ Impala 실행 스크립트 실행 (올바른 실행 방식 사용)
CMD ["/opt/impala/bin/start-impala-cluster"]

