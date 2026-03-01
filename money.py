import asyncio
import time

from nicegui import ui


def generate_report():
    time.sleep(2)
    return 'Report generated successfully'


def run_report():
    result = generate_report()
    ui.notify(result)



async def sync_data():
    for i in range(5):
        await asyncio.sleep(1)
        progress.value += 20
    ui.notify('Data sync complete')

progress = ui.linear_progress(value=0)
ui.button('Start Sync', on_click=lambda: asyncio.create_task(sync_data()))





ui.button('Generate Report', on_click=run_report)

ui.label('Money')

ui.run(port=8000)

