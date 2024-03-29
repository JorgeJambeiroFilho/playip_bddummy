FROM tiangolo/uvicorn-gunicorn-fastapi:python3.10

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD . /app
ADD ./dependencies_copy /app
 # Install any needed packages
#RUN python setup.py develop

RUN pip install -r requirements.txt


