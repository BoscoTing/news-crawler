import random
from typing import Union, Tuple
import asyncio


def parse_delay_range(
    delay: Union[int, float, Tuple[Union[int, float], Union[int, float]]]
) -> Tuple[float, float]:
    """解析延遲範圍參數

    Args:
        delay: 可以是單一數值或表示範圍的元組
              如果是單一數值，則範圍為 0 到該數值
              如果是元組，則為 (最小值, 最大值)

    Returns:
        Tuple[float, float]: 延遲的最小值和最大值（秒）
    """
    if isinstance(delay, (int, float)):
        return 0.0, float(delay)
    elif isinstance(delay, tuple) and len(delay) == 2:
        min_delay, max_delay = float(delay[0]), float(delay[1])
        if min_delay > max_delay:
            min_delay, max_delay = max_delay, min_delay
        return min_delay, max_delay
    else:
        raise ValueError("delay 必須是數值或者包含兩個數值的元組")


async def random_delay_async(
    delay: Union[int, float, Tuple[Union[int, float], Union[int, float]]] = (1, 3)
) -> float:
    """非同步版本：生成隨機延遲並等待

    Args:
        delay: 延遲時間設定，可以是：
              - 單一數值：將在 0 到該值之間隨機延遲
              - 元組：將在兩個值之間隨機延遲
              默認值是 (1, 3) 秒

    Returns:
        float: 實際延遲的秒數

    Example:
        >>> delay_time = await random_delay_async()  # 等待 1-3 秒
        >>> delay_time = await random_delay_async(2)  # 等待 0-2 秒
        >>> delay_time = await random_delay_async((0.5, 1.5))  # 等待 0.5-1.5 秒
    """
    min_delay, max_delay = parse_delay_range(delay)
    delay_seconds = random.uniform(min_delay, max_delay)
    await asyncio.sleep(delay_seconds)
    return delay_seconds
