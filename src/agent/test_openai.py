from openai import OpenAI

client = OpenAI(
  api_key="sk-fwXw6ukfAtOihdt5PO_fJg",
  base_url="https://litellm.ai.paas.htec.rs"
)

completion = client.chat.completions.create(
  model="l2-gpt-4o",
  store=True,
  messages=[
    {"role": "user", "content": "Besiktas kimdir?"}
  ]
)

print(completion.choices[0].message);
