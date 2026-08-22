FROM public.ecr.aws/lambda/python:3.12

RUN pip install --no-cache-dir lancedb==0.37.1

COPY handler.py ${LAMBDA_TASK_ROOT}/

CMD ["handler.lambda_handler"]
