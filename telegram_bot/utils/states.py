from aiogram.fsm.state import State, StatesGroup

class SettingsStates(StatesGroup):
    wait_for_spread = State()
    wait_for_work_vol = State()
    wait_for_volume_24h = State()
    wait_for_lifetime = State()
    wait_for_convergence = State()
