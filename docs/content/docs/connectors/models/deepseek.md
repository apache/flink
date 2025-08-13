# DeepSeek

The DeepSeek Model Function allows Flink SQL to call [DeepSeek API](https://www.deepseek.com/) for inference tasks.

## Configuration Options

| Option         | Required | Default  | Description                                                         |
|----------------|----------|----------|---------------------------------------------------------------------|
| provider       | Yes      | -        | Must be `deepseek`                                                  |
| endpoint       | Yes      | -        | API endpoint (e.g., `https://api.deepseek.com/v1/chat/completions`) |
| api-key        | Yes      | -        | DeepSeek API key                                                    |
| model          | Yes      | -        | Model name (e.g., `deepseek-chat`、`deepseek-reasoner`)                                 |
| temperature    | No       | 0.7      | Sampling temperature                                                |
| max-tokens     | No       | -        | Maximum output tokens                                               |
| dimension      | No       | -        | Embedding vector dimension                                          |

## Usage Example

```sql
-- Create chat model
CREATE MODEL deepseek_chat
INPUT (input STRING)
OUTPUT (content STRING)
WITH (
  'provider' = 'deepseek',
  'endpoint' = 'https://api.deepseek.com/v1/chat/completions',
  'api-key' = 'your-api-key',
  'model' = 'deepseek-chat',
  'temperature' = '0.5'
);

-- Use model for prediction
SELECT input, content
FROM ML_PREDICT(TABLE source_table, MODEL deepseek_chat, DESCRIPTOR(`input`));
