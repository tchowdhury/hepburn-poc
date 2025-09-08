FROM public.ecr.aws/lambda/python:3.12

# Copy requirements.txt
COPY app/requirements.txt ${LAMBDA_TASK_ROOT}

# Install the specified packages
RUN pip install -r requirements.txt

# Copy the application code
COPY app/ ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler.
CMD ["move_to_landing.lambda_handler"]