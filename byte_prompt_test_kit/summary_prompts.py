
###### sector specific examples of summarised articles ######

insurance_example_summaries = [
    """
   </example>
   • Allianz UK has developed "Incognito," a machine-learning tool designed to identify potentially fraudulent insurance claims, which are then reviewed by fraud experts.
   • "Incognito" has saved £1.7m to date with an additional £3.4m held in claim reserves, demonstrating its effectiveness in Allianz's fraud prevention strategy.
   • James Burge, head of Counter-Fraud at Allianz Commercial, stated, "The development of "Incognito" has ensured that we enhance the market-leading service that we provide to our customers. We have been able to settle claims quicker and identify fraud at the earliest opportunity.”
   </example>

   </example>
   •Sixfold, a generative AI company specialised in insurance risk analysis, launched "Discrepancy Scan" to automate the detection of inconsistencies in life and disability insurance applications. It works by comparing applicant-provided information with medical records, aiming to flag undisclosed medications and other mismatches.
   •Sixfold's AI operates through a four-step process: scanning/ingesting documentation, comparing disclosures against verified data, flagging meaningful mismatches, and tracking records in real-time, with plans to expand beyond medications to include diagnoses, procedures, and lifestyle disclosures.
   •Insurers are increasingly adopting AI to improve operational performance, address challenges like applicant misrepresentation and human error, and combat insurance fraud, which costs the industry billions annually.
   </example>

   </example>
   •Aon, a major player in the insurance and reinsurance industry, views the opportunities presented by artificial intelligence (AI) as “real and meaningful,” aiming to overcome challenges posed by volatility through analytics, content, and insights where generative AI plays a pivotal role.
   •Aon has committed a $1 billion investment to advance generative AI and its application in analytics, content, and capabilities, with the goal of strengthening client experience and supporting business growth across various applications including content, service, and supply chain support.
   •Generative AI, coupled with analytics and insights, is enabling Aon to access broader sets of capital, moving beyond the traditional $4 trillion global insurance risk pool to tap into a $250 trillion capital pool from sources like sovereign wealth funds, pension funds, and high-net-worth investors, particularly for parametric or specialised instruments.
   </example>

   </example>
   •Industry players Nationwide are actively deploying artificial intelligence in their operations through partnerships with companies like CompScience and Swiss Re to enhance risk management.
   •Nationwise is utilising computer vision AI to analyse workplace environments and identify potential hazards in real-time, influencing how insurers assess risk and support prevention efforts on the ground.
   •This AI-driven approach has led to a 23% reduction in workers' compensation claims for Nationwide, demonstrating AI's practical benefits in risk assessment and mitigation.
   </example>
   """
]

banking_example_summaries = [
    """
<example>
•U.S. Bank, which revealed today that it has made 2,000 such loans in a matter of months with fintech partner Pagaya, and Suncoast Credit Union, which has been working with Zest AI to lend to underserved communities.
•The bank deployed Zest AI software a couple of years ago. It takes the more basic decisions away from the human loan analysts, giving them more time in the day to concentrate on the most challenging applications, looking for opportunities to serve members and offer coaching and counseling, Johnson said.
•When U.S. Bank runs a loan application through its usual underwriting model and gets a decline, it immediately and automatically sends the application to Pagaya, which runs it through its AI-based model.
</example>

<example>
•The assessment of credit risk demands a great deal of manual effort. A discussion with Simone Mensink (Director of Banking at IG&H) and Hein Wegdam (Head of ING Real Estate) on the credit assessment of the future, which combines expert knowledge with artificial intelligence.
•At banks, the asset-based finance department often deals with credit checks high in complexity. The department deals with both low-risk and high-risk customer profiles that require a lot of manual handling.
•Pointing at a practical example, Wegdam said: “Together with the data science team of IG&H we developed unique decision models for the real estate financing market for loan reviews, extensions and applications. 80% of reviews and 50% of loan extensions were automated. We create added value by using our real estate financing knowledge where specific expertise is needed, like risk exceptions.”
</example>

<example>
•CBA said that it had practiced “responsible scaling of AI, resulting in [the] 50-plus generative AI use cases to simplify operational processes and support our frontline to serve customers” materialising between June and November last year.
•These use cases, it said, were generated out of CommBank Gen.ai Studio, an H2O.ai powered environment aimed at enabling safe experimentation with large language models (LLMs).
•In addition, CBA suggested that generative AI is enabling it to experiment more with its long-running next best conversation (NBC) engine, known as the customer engagement engine or CEE.
•Though somewhat cryptic, CBA indicated it had seen a “30x increase in experimentation capability within an NBC compared to [the] current CEE A/B testing framework with GenAI.”
</example>
   """
]

#TODO: add to documentation for maintenance
example_summaries_lookup = {
    "Index Bank": banking_example_summaries,
    "Index Insurance": insurance_example_summaries,
}

def get_summary_prompt(sector):

   """
    Constructs a sector-specific summarisation prompt for an AI article summariser.

    This function retrieves example summaries for the given sector and uses them to 
    generate a detailed prompt.
   """
   if sector not in example_summaries_lookup:
        raise ValueError(f"Sector '{sector}' not found in example_summaries_lookup. Available sectors: {list(example_summaries_lookup.keys())}")
   example_summaries = example_summaries_lookup[sector]
   example_summaries = "\n".join([example.strip() for example in example_summaries])

   prompt = f""" You are tasked with summarizing an article about AI use in the {sector} sector, particularly focusing on what the {sector} sector is doing with AI. Your summary should be in the form of concise bullet points. Here's how to approach this task:

      1. First, carefully read the following article:
      <article>
      {{}}
      </article>

      2. As you analyze the article, focus on the following key areas:
         - AI initiatives by {sector} companies, {sector} institutions, {sector} providors
         - Hiring of AI specialists by {sector} companies
         - Specific AI use cases in the {sector} sector
         - AI training programs for employess in the {sector} sector
         - AI regulations in the {sector} sector
         - AI startups providing services to {sector} institutions
         - Any other information relevant to AI in {sector}
         - If there is absolutely nothing directly relevant to AI in {sector}, you can summarise the general AI points in the article

      3. Create a bullet point summary using the following guidelines:
         - Use the • symbol to denote each bullet point
         - Start each bullet point on a new line
         - Do not leave empty lines between bullet points
         - Aim strongly for 3 high-quality bullet points, but include more if the article contains abundant relevant information
         - Make the bullet points concise, informative, and straight to the point
         - If there are noteworthy quotes, include them word-for-word within the relevant bullet point

      4. When writing your summary, consider what would be most useful for someone providing AI services to major {sector} companies to know when meeting and advising clients.

      5. Present your summary in the following format:
      • [First bullet point]
      • [Second bullet point]
      • [Third bullet point]
      [Additional bullet points if necessary]


      Here are some bullet points I have generated from other articles, try to replicate the tone and writing pattern of these examples:

      {example_summaries}

      Remember to keep your summary focused, informative, and relevant to AI use in the {sector} sector, particularly emphasizing {sector} companies' and institutions' activities and strategies related to AI.

      No additional prose alongside the bullet points"""

   return prompt


SYSTEM_PROMPT = "You are an editor with 20 years experience at the New York Times. You are very skilled at producing bullet point summaries of articles"
