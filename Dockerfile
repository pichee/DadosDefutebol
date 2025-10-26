FROM python:3.11

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y default-jdk && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

ENV PATH=$JAVA_HOME/bin:$PATH

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY Scripts/. ./Scripts/.

CMD ["/bin/bash", "-c", "python Scripts/Trusted_To_Refined/Classificacao.py"]
