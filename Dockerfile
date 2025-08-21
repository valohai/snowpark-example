# Use official Python image as base
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy app code
COPY insurance_streamlit/ .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt


# Expose Streamlit default port
EXPOSE 8501

# Run Streamlit app
CMD ["streamlit", "run", "home.py"]