import os
import json
import pandas as pd
from flask import Flask, request
from google.cloud import storage
from functions_wrapper import entrypoint
from Process_1 import process_excel_data
from Process_2 import generate_excel
import tempfile

temp_dir = tempfile.gettempdir()
app = Flask(__name__)


################# Test ############################

@app.route('/', methods=['GET'])
def get():
    return "I am Running!!"


##################### Service_code_1 ######################

@app.route('/p1', methods=['POST'])
def service_code_1():
    try:
        if request.method == 'POST':
            file1 = request.files['source_file']
            auth = request.headers.get('authorization')
            if auth == os.environ.get('auth_code'):
                columns_mapped, columns = process_excel_data(file1)
                output = {'mapped_columns': columns_mapped, 'columns': columns}
                response_obj = app.response_class(response=json.dumps(output), status=200, mimetype='application/json')
            else:
                response_obj = app.response_class(response="Authentication Failed", status=401,
                                                  mimetype='application/json')
            return response_obj
    except Exception as e:
        data = {'status': 'failed', 'message': str(e)}
        response_obj = app.response_class(response=json.dumps(data), status=500, mimetype='application/json')
        return response_obj


##################### service_code_2 ######################

@app.route('/p2', methods=['POST'])
def service_code_2():
    try:
        if request.method == 'POST':
            file1 = request.files['source_file']
            res_map_str = request.form.get('result')
            print('testing', res_map_str)
            out_path = request.form.get('id')
            auth = request.headers.get('authorization')
            if auth == os.environ.get('auth_code'):
                # Parse res_map
                res_map = json.loads(res_map_str)
                print('testing', res_map)

                # Download existing CSV from the bucket
                storage_client = storage.Client()
                bucket = storage_client.bucket('alex_uploads')
                blob = bucket.blob('knowledge_base/res1.csv')
                existing_csv_path = f'{temp_dir}/res1.csv'
                blob.download_to_filename(existing_csv_path)

                try:
                    # Read existing CSV
                    existing_df = pd.read_csv(existing_csv_path)
                except pd.errors.EmptyDataError:
                    # If CSV is empty, create a new DataFrame
                    existing_df = pd.DataFrame()

                # Append new data
                new_data = pd.DataFrame([res_map])
                updated_df = pd.concat([existing_df, new_data], ignore_index=True)

                # Save the updated CSV
                updated_csv_path = f'{temp_dir}/res1_updated.csv'
                updated_df.to_csv(updated_csv_path, index=False)

                # Upload the updated CSV back to the bucket
                blob.upload_from_filename(updated_csv_path)

                # Save and process the uploaded file (existing code)
                file1.save(file1.filename)
                output = generate_excel(file1, res_map)
                excel_final_path = f'{temp_dir}/result.xlsx'
                output.to_excel(excel_final_path, index=True)
                blob = bucket.blob(f'{out_path}/result.xlsx')
                blob.upload_from_filename(excel_final_path)
                print(f"File {output} uploaded to {out_path}/result.xlsx.")
                response_obj = app.response_class(response="Success!!", status=200, mimetype='application/json')
            else:
                response_obj = app.response_class(response="Authentication Failed", status=401,
                                                  mimetype='application/json')
            return response_obj
    except Exception as e:
        data = {'status': 'failed', 'message': str(e)}
        response_obj = app.response_class(response=json.dumps(data), status=500, mimetype='application/json')
        return response_obj


app_wrap = lambda request: entrypoint(app, request)

if __name__ == '__main__':
    app.run(use_reloader=True, debug=True, host='0.0.0.0', port=5858)
