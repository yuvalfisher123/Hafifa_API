FROM python:alpine

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 5000

CMD [ "flask", "--app", "api.py", "run", "--host", "0.0.0.0"]