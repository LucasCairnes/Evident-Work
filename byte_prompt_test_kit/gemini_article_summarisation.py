from google import genai
from google.genai import types

# ---- Gemini Summariser ----
class GeminiArticleSummariser:
    def __init__(self,
                 summarisation_prompt,
                 model_name = "google/gemini-2.5-pro",
                 project_id=None):
        self.summarisation_prompt = summarisation_prompt
        self.model_name = model_name
        self.project_id = project_id
        

        # gemini client and configuration
        self.google_genai_client = genai.Client(
            vertexai=True,
            project=project_id,
            location="europe-west1",
        )

        # model config
        self.model_configuration = types.GenerateContentConfig(
            system_instruction=summarisation_prompt,
            response_mime_type="text/plain",
            max_output_tokens=20000
        )

    async def generate_summary(self, article_text):
        try:
            # Format the input
            user_input = (f"You are an editor with 20 years experience at the New York Times."
                         f"You are very skilled at producing bullet point summaries of articles."
                         f"Please summarise the following article using the instructions below:\n\n<article>{article_text}</article>"
            )
            # Send the request
            response = await self.google_genai_client.aio.models.generate_content(
                model=self.model_name,
                config=self.model_configuration,
                contents=[{"role": "user", "parts": [{"text": user_input}]}],
            )

            # Extract response
            print(f"Response: {response}")
            summary = response.candidates[0].content.parts[0].text
        
            return summary.strip()

        except Exception as e:
            print(f"Error summarising article: {e}")
            return None

    def clean_summary(self, summary):
        
        summary = summary.replace('•', '\n•')
        summary = summary.strip()
        summary = summary.replace('\n\n', '\n')
        summary = summary.replace('\n\n', '\n')
        summary = summary.replace('• ', '•')
        summary = summary.removeprefix('\n')

        return summary