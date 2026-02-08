import uuid

from datetime import timedelta

from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio import activity

import asyncio
import dotenv

dotenv.load_dotenv()

from pydantic_ai import Agent
from pydantic_ai.durable_exec.temporal import (
    PydanticAIPlugin,
    PydanticAIWorkflow,
    TemporalAgent,
)

# TODO Still getting type errors
agent = Agent(
    'ollama:functiongemma:270m',
    instructions="You're an expert in geography.",
    output_type=str,
    name='geography',
)

temporal_agent = TemporalAgent(agent)

@activity.defn
async def call_agent(prompt: str) -> str:
    return await temporal_agent.run(prompt)

@workflow.defn
class GeographyWorkflow(PydanticAIWorkflow):
    __pydantic_ai_agents__ = [temporal_agent]

    @workflow.run
    async def run(self, prompt: str) -> str:
        return await workflow.execute_activity(
            call_agent,
            args=[prompt],
            start_to_close_timeout=timedelta(minutes=1),
        )

async def main():
    client = await Client.connect(
        'localhost:7233',
        plugins=[PydanticAIPlugin()],
    )

    async with Worker(
        client,
        task_queue='geography',
        workflows=[GeographyWorkflow],
        activities=[call_agent],
    ):
        output = await client.execute_workflow(
            GeographyWorkflow.run,
            args=['What is the capital of Mexico?'],
            id=f'geography-{uuid.uuid4()}',
            task_queue='geography',
        )
        print(output)

if __name__ == "__main__":
    asyncio.run(main())