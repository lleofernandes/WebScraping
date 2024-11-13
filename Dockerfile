# usa uma imagem base do python
FROM python:3.12-slim

#define o diretorio de trabalho dentro do conteiner
WORKDIR /app

#copia o arquivo de dependencias para o diretorio de trabalho
COPY requirements.txt .

#instala as dependencias do python
RUN pip install --no-cache-dir -r requirements.txt

#copia o codigo de aplicacao para o container
COPY . .

#instrucao CMD para rodar o aplicativo
CMD ["python", "app.py"]
