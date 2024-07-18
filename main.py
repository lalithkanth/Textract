from flask import Flask, request, jsonify
import boto3
import time

app = Flask(__name__)

@app.route('/process_document', methods=['POST'])
def process_document():
    data = request.json
    s3_bucket_name = data.get('s3_bucket_name')
    object_name = data.get('object_name')

    if not s3_bucket_name or not object_name:
        return jsonify({"error": "s3_bucket_name and object_name are required."}), 400

    client = boto3.client(
        'textract',
        aws_access_key_id='AKIAYS2NTH5DBWXE57F3',
        aws_secret_access_key='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',
        region_name='eu-west-2'
    )

    try:
        # Start the text detection job
        response = client.start_document_text_detection(
            DocumentLocation={
                'S3Object': {
                    'Bucket': s3_bucket_name,
                    'Name': object_name
                }
            }
        )

        if "JobId" not in response:
            return jsonify({"error": "Failed to start job, no JobId returned."}), 500
        
        job_id = response["JobId"]
        print("Started job with id: {}".format(job_id))

        # Check job status and collect results
        print("Checking job status...")
        while True:
            response = client.get_document_text_detection(JobId=job_id)
            status = response["JobStatus"]
            print("Job status: {}".format(status))

            if status == "SUCCEEDED":
                break
            elif status in ["FAILED", "PARTIAL_SUCCESS"]:
                return jsonify({"error": "Job failed or partial success."}), 500
            
            time.sleep(5)  # Wait before polling again

        # Collect results
        pages = [response]
        while 'NextToken' in response:
            response = client.get_document_text_detection(JobId=job_id, NextToken=response['NextToken'])
            pages.append(response)

        # Extract and return results
        results = []
        for result_page in pages:
            for item in result_page["Blocks"]:
                if item["BlockType"] == "LINE":
                    results.append(item["Text"])

        return jsonify({"results": results})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)