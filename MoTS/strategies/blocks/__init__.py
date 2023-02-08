# from .netmode import NetMODEMotifCounter, NetMODEBlockMotifCounter
from .high_order import HighOrderBlockMotifCounter, HighOrderMotifCounter

BLOCK_MOTIF_COUNTER = {
    # 'netmode': NetMODEBlockMotifCounter,
    'highorder': HighOrderBlockMotifCounter,
}
