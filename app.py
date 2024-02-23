from flask import Flask

app = Flask(__name__)

@app.route('/validate_payment', methods=['GET'])
def validate_payment():
    return 'Payment is valid'

if __name__ == '__main__':
    app.run(debug=True)
