FROM public.ecr.aws/lambda/python:3.12

# pandas + pyarrow together are near Lambda's 250MB zip/Layer limit, hence
# the Image package type (same pattern as write_lancedb). boto3 ships in the
# base image.
RUN pip install --no-cache-dir pandas==3.0.5 pyarrow==25.0.1 duckdb==1.5.5

COPY handler.py ${LAMBDA_TASK_ROOT}/

CMD ["handler.lambda_handler"]
