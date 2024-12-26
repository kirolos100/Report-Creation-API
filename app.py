from flask import Flask, request, jsonify
import json
from openai import AzureOpenAI
from flasgger import Swagger, swag_from
from flask_swagger_ui import get_swaggerui_blueprint
from flask_cors import CORS  # Import CORS


# Initialize Azure OpenAI
llm = AzureOpenAI(
    azure_endpoint="https://genral-openai.openai.azure.com/",
    api_key="8929107a6a6b4f37b293a0fa0584ffc3",
    api_version="2024-02-01"
)

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # This will allow cross-origin requests to all routes


# Swagger UI setup
SWAGGER_URL = '/api/docs'  # URL for exposing Swagger UI (without trailing '/')
API_URL = '/static/swagger.json'  # Our API url (can of course be a local resource)

swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,  # Swagger UI static files will be mapped to '{SWAGGER_URL}/dist/'
    API_URL,
    config={  # Swagger UI config overrides
        'app_name': "AutoML APIs"
    }
)

app.register_blueprint(swaggerui_blueprint)

# Swagger configuration
swagger = Swagger(app, template={
    "info": {
        "title": "AutoML APIs",
        "description": "API for Automated Machine Learning tool",
        "version": "1.0.0"
    },
    "host": "ndcreportcreationapi-fje6fhfcgehhfgdt.eastus-01.azurewebsites.net",  # Change to your host if needed
    "basePath": "/",  # Base path for API
})

# Helper function to generate Arabic report
def generate_arabic_report(
    موضوع_التقرير,
    منظور_التقرير,
    الجمهور_المستهدف,
    النقاط_والجوانب_الهامة,
    إضافة_مصادر
):
    نقاط = "\n".join(f"- {نقطة}" for نقطة in النقاط_والجوانب_الهامة)
    المصادر = (
        "\n\nيرجى استخدام المصادر التالية لدعم التقرير:\n" + "\n".join(إضافة_مصادر)
        if إضافة_مصادر
        else ""
    )

    arabic_prompt = f"""
    اكتب تقريرًا باللغة العربية يحمل عنوانا عن: "{موضوع_التقرير}". 
    يجب أن يتم كتابة التقرير من منظور: {', '.join(منظور_التقرير)}. 
    الجمهور المستهدف للتقرير هم: {الجمهور_المستهدف}. 
    النقاط والجوانب الهامة التي يجب تناولها:
    {نقاط}
    {المصادر}
    انا اريد json file مفصلا و وشارحا باستفاضة بدون ذكر المصدر في كل نقطة يحتوي على:
      headings: [
          version: 1,
          title: عنوان فرعي لحميع النقاط,
          listItemsList: [
              listItems: [
                  title: عنوان النقطة,
                  content: محتوى تلك النقطة مكتوبا ب HTML tags
    المقال يجب ان يحتوي على اكثر من heading باكثر من عنوان فرعي و اكثر من النقطة
لا تقوم بتغيير هذا ال Format الذي يبدا ب list of headings.
    لا تقوم بتغيير ترتيب ال format 
لاحظ اني اريد heading يحتوي على مجموعة titles و كل title له content مفصل 
و انا اريد اكثر من heading    
    """

    conversation_history = [
        {
            "role": "system",
            "content": """You are a professional journalist writing an article in a detailed json file about the user input.
                          Use the provided context, write in Arabic, and create a json report with details and informations between 1000 and 1500 words.
                          Incorporate the sources given in the prompt when relevant."""
        },
        {"role": "user", "content": arabic_prompt}
    ]

    try:
        response = llm.chat.completions.create(
            model="gpt-4o",
            messages=conversation_history
        ).choices[0].message.content
        print("Raw response:", response)

        cleaned_response = response.strip()

        if not cleaned_response:
            raise ValueError("Received an empty response from the model")

        # Remove the code block markers
        if cleaned_response.startswith("```json"):
            cleaned_response = cleaned_response[len("```json"):].strip()
        if cleaned_response.endswith("```"):
            cleaned_response = cleaned_response[:-3].strip()

        # Check if the cleaned response is valid JSON
        structured_json = json.loads(cleaned_response)
        return structured_json
    except Exception as e:
        print(f"Error while processing the response: {e}")
        return None


@app.route('/generate_report', methods=['POST'])
@swag_from({
    "tags": ["Generate Report"],
    "description": "Generate a detailed Arabic report based on the provided inputs.",
    "parameters": [
        {
            "name": "body",
            "in": "body",
            "required": True,
            "description": "Input data in JSON format to generate a report.",
            "schema": {
                "type": "object",
                "properties": {
                    "موضوع_التقرير": {"type": "string"},
                    "منظور_التقرير": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "الجمهور_المستهدف": {"type": "string"},
                    "النقاط_والجوانب_الهامة": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "إضافة_مصادر": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                }
            }
        }
    ],
    "responses": {
        "200": {
            "description": "Report generated successfully.",
            "schema": {
                "type": "object",
                "properties": {
                    "report": {"type": "object"}
                }
            }
        },
        "500": {
            "description": "Error occurred while generating the report."
        }
    }
})
def generate_report():
    """
    Generate a detailed Arabic report based on user input.
    """
    try:
        data = request.get_json()
        موضوع_التقرير = data.get('موضوع_التقرير')
        منظور_التقرير = data.get('منظور_التقرير', [])
        الجمهور_المستهدف = data.get('الجمهور_المستهدف')
        النقاط_والجوانب_الهامة = data.get('النقاط_والجوانب_الهامة', [])
        إضافة_مصادر = data.get('إضافة_مصادر', [])

        report = generate_arabic_report(
            موضوع_التقرير,
            منظور_التقرير,
            الجمهور_المستهدف,
            النقاط_والجوانب_الهامة,
            إضافة_مصادر
        )
        return jsonify({"report": report})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Root route for testing
@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

if __name__ == '__main__':
    app.run(debug=True)
