from flask import Flask, request, jsonify
import json
import requests
from bs4 import BeautifulSoup
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
CORS(app)  # Allow cross-origin requests

# Swagger UI setup
SWAGGER_URL = '/api/docs'
API_URL = '/static/swagger.json'

swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={'app_name': "AutoML APIs"}
)
app.register_blueprint(swaggerui_blueprint)

swagger = Swagger(app, template={
    "info": {
        "title": "AutoML APIs",
        "description": "API for Automated Machine Learning tool",
        "version": "1.0.0"
    },
    "host": "https://ndcreportcreationapi-fje6fhfcgehhfgdt.eastus-01.azurewebsites.net",
    "basePath": "/",
})

# Helper function to fetch URLs from the external API
def fetch_urls(موضوع_التقرير, منظور_التقرير):
    try:
        api_endpoint = "https://ndc-bing-search-hrhra6fkcuaffjby.canadacentral-01.azurewebsites.net/bing_search"
        payload = {
            "منظور_التقرير": منظور_التقرير,
            "موضوع_التقرير": موضوع_التقرير
        }
        headers = {"accept": "application/json", "Content-Type": "application/json"}
        response = requests.post(api_endpoint, json=payload, headers=headers)
        response.raise_for_status()
        return response.json().get("URLs", [])
    except Exception as e:
        print(f"Error fetching URLs: {e}")
        return []

# Function to fetch content from a URL
def fetch_url_content(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        return soup.get_text(separator="\n", strip=True)
    except Exception as e:
        print(f"Error fetching content from {url}: {e}")
        return None

# Helper function to generate Arabic report
def generate_arabic_report(
    موضوع_التقرير,
    منظور_التقرير,
    الجمهور_المستهدف,
    النقاط_والجوانب_الهامة,
    sources
):
    نقاط = "\n".join(f"- {نقطة}" for نقطة in النقاط_والجوانب_الهامة)
    محتويات_المصادر = []
    
    # Fetch content for each source
    for source in sources:
        content = fetch_url_content(source)
        if content:
            محتويات_المصادر.append(f"Source: {source}\nContent: {content}")
    
    المصادر = "\n\n".join(محتويات_المصادر)

    arabic_prompt = f"""
    اعطني هيكل لتقرير باللغة العربية يحمل عنوانًا عن: "{موضوع_التقرير}". 
    يجب أن يتم كتابة التقرير من منظور: {', '.join(منظور_التقرير)}. 
    الجمهور المستهدف للتقرير هم: {الجمهور_المستهدف}. 
    النقاط والجوانب الهامة التي يجب تناولها:
    {نقاط}
    
    يرجى تضمين إحصائيات وتحليلات مفصلة في كل نقطة، وشرح وافٍ بالمحتوى العلمي مع تقسيم المقال إلى أكثر من عنوان فرعي. استخدم البيانات التالية من المصادر لدعم المحتوى:
    {المصادر}    

انا اريد json file مفصلا و وشارحا باستفاضة بدون ذكر المصدر في كل نقطة يحتوي على:
      headings: [ لاحظ ان الheadings تحتوي على list كبيرة التي تحتوي على listItmesList list & listItems list
          version: 1,
          title: عنوان فرعي لحميع النقاط,
          listItemsList: [
              listItems: [
                  title: عنوان النقطة,
                  content: محتوى تلك النقطة مكتوبا ب HTML tags انا اريد جملة واحدة فقط لا غير توضح تلك محتوى تلك النقطة و تلك الجملة يجب ان تبدا ب "هذه النقطة لهذا العنوان ستتحدث عن ..." يجب ان يكون محتوى تلك النقطة يتحدث ايضا عن توقعاتك المستقبلية في تلك الجملة الواحدة و في ظل ذلك الموضوع لذلك التقرير فيجب ايضا ان تضيف على الجملة الواحدة و ليس بدلا منها "و ايضا ان تلك النقطة ستتحدث عن التوقعات ..."
                            جميع النقاط يجب ان تحمل توقعات مستقبلية
    المقال يجب ان يحتوي على اكثر من heading باكثر من عنوان فرعي و اكثر من النقطة
لا تقوم بتغيير هذا ال Format الذي يبدا ب list of headings.
    لا تقوم بتغيير ترتيب ال format 
لاحظ ان الheadings تحتوي على list كبيرة التي تحتوي على listItmesList list & listItems list
لاحظ اني اريد heading يحتوي على مجموعة titles و كل title له content مفصل 
لا تنسى عنوان او ال title لكل heading فانا اريد عنوان رئيسي لكل النقاط التي تكون بداخل ذلك ال heading و داخل كل heading يوجد عناوين فرعية و محتوى او content لكل نقطة منهم
و انا اريد اكثر من heading    
    """
    print(arabic_prompt)
    conversation_history = [
        {
            "role": "system",
            "content": "You are a professional journalist tasked with writing a detailed informative and valuable Arabic article in JSON format not in String Format. The output should contain detailed statistics and analysis for every point."
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
                    "Report_Topic": {"type": "string"},
                    "Report_Perspective": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "Target_audience": {"type": "string"},
                    "Important_points_and_aspects": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "Add_Resources": {
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
        موضوع_التقرير = data.get('Report_Topic')
        منظور_التقرير = data.get('Report_Perspective', [])
        الجمهور_المستهدف = data.get('Target_audience')
        النقاط_والجوانب_الهامة = data.get('Important_points_and_aspects', [])
        إضافة_مصادر = data.get('Add_Resources', [])
        # Fetch URLs and content
        api_urls = fetch_urls(موضوع_التقرير, منظور_التقرير)
        all_sources = list(set(api_urls + إضافة_مصادر))

        report = generate_arabic_report(
            موضوع_التقرير,
            منظور_التقرير,
            الجمهور_المستهدف,
            النقاط_والجوانب_الهامة,
            إضافة_مصادر
        )
        return report, 200
    except Exception as e:
        report = generate_arabic_report(
            موضوع_التقرير,
            منظور_التقرير,
            الجمهور_المستهدف,
            النقاط_والجوانب_الهامة,
            إضافة_مصادر
        )
        return report, 200

# Root route for testing
@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"

if __name__ == '__main__':
    app.run(debug=True)
