# Render 等 Linux 部署：安装 Microsoft ODBC Driver 17，供 pyodbc 连接 Wind SQL Server
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl gnupg apt-transport-https ca-certificates \
        unixodbc unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Render 注入 PORT（默认 10000）；exec 便于进程收 SIGTERM；启动 echo 便于区分新旧容器日志
CMD ["sh", "-c", "echo render-start uvicorn port=${PORT:-10000} && exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-10000} --log-level info"]
