from flask import Flask, request, jsonify
import worker

app = Flask(__name__)

@app.route('/pubsub/push', methods=['POST'])
def pubsub_push():
    envelope = request.get_json()
    if not envelope:
        msg = 'No Pub/Sub message received'
        print(f'Error: {msg}')
        return jsonify({'error': msg}), 400

    if not isinstance(envelope, dict) or 'message' not in envelope:
        msg = 'Invalid Pub/Sub message format'
        print(f'Error: {msg}')
        return jsonify({'error': msg}), 400

    pubsub_message = envelope['message']
    try:
        success = worker.handle_pubsub_message(pubsub_message)
        if success:
            return '', 204
        else:
            return jsonify({'error': 'Failed to process message'}), 500
    except Exception as e:
        print(f'Error processing message: {e}')
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
