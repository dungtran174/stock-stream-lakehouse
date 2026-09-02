from flask import Flask, request, jsonify
import pandas as pd
import os

app = Flask(__name__)

@app.route('/api/get_data', methods=['GET'])
def get_data():
    try:
        year = request.args.get('year')
        month = request.args.get('month')
        day = request.args.get('day')
        offset = int(request.args.get('offset', 0))
        limit = int(request.args.get('limit', 100))

        file_path = f'./data/stock_transactions_{year}_{month}_{day}.csv'
        if not os.path.exists(file_path):
            return jsonify({'status': 'error', 'data': 'Data file not found'})

        df = pd.read_csv(file_path)
        cnt = len(df)

        if offset >= cnt:
            return jsonify({'status': 'complete', 'data': []})

        if offset + limit >= cnt:
            result = df.iloc[offset:]
            return jsonify({'status': 'complete', 'data': result.to_dict(orient='records')})

        result = df.iloc[offset:offset+limit]
        return jsonify({'status': 'success', 'data': result.to_dict(orient='records')})
    except Exception as e:
        print(f"Error fetching data: {e}")
        return jsonify({'status': 'error', 'data': str(e)})

if __name__ == '__main__':
    # Listen on all interfaces so it can be accessed inside Docker
    app.run(host='0.0.0.0', debug=True, port=5000)
