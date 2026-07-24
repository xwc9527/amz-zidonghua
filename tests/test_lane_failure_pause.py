"""覆盖 fetch_products.py 的 lane（固定代理通道）暂停判定：
lane_pause_reason 除了原有的验证码占比暂停外，新增"非验证码普通失败率"暂停
（连接超时/连接被拒等），用于识别刚从代理池冷启动补入热池、还没被真实
Amazon流量验证过、实际扛不住并发的出口——避免它一直占着worker反复重试
到耗尽attempts，拖慢/拖垮整轮抓取。同时保证不会把最后一条活跃lane也暂停
掉，否则任务队列会无人消费而死锁。
"""
from __future__ import annotations

import fetch_products as fp


def _st(total=0, captcha=0, failed=0, paused=False):
    return {"total": total, "captcha": captcha, "failed": failed, "paused": paused}


def test_no_pause_below_min_samples():
    # 样本量不足（< PRODUCT_LANE_FAILURE_MIN_SAMPLES），即使全失败也不暂停。
    st = _st(total=fp.PRODUCT_LANE_FAILURE_MIN_SAMPLES - 1,
              failed=fp.PRODUCT_LANE_FAILURE_MIN_SAMPLES - 1)
    assert fp.lane_pause_reason(st, active_lane_count=5) == ""


def test_pause_on_high_failure_rate_non_captcha():
    # 样本量够、失败率达到阈值、且全是非验证码失败（timeout等）。
    total = fp.PRODUCT_LANE_FAILURE_MIN_SAMPLES
    failed = int(total * fp.PRODUCT_LANE_FAILURE_PAUSE_RATE) + 1
    failed = min(failed, total)
    st = _st(total=total, captcha=0, failed=failed)
    assert fp.lane_pause_reason(st, active_lane_count=5) == "failure_rate"


def test_no_pause_when_failure_rate_below_threshold():
    total = fp.PRODUCT_LANE_FAILURE_MIN_SAMPLES + 10
    # 失败率明显低于阈值——偶发性失败不应触发暂停。
    failed = 1
    st = _st(total=total, captcha=0, failed=failed)
    assert fp.lane_pause_reason(st, active_lane_count=5) == ""


def test_captcha_pause_takes_priority_and_still_works():
    total = fp.PRODUCT_LANE_CAPTCHA_MIN_SAMPLES
    captcha = int(total * fp.PRODUCT_LANE_CAPTCHA_PAUSE_RATE) + 1
    captcha = min(captcha, total)
    st = _st(total=total, captcha=captcha, failed=captcha)
    assert fp.lane_pause_reason(st, active_lane_count=5) == "captcha"


def test_already_paused_lane_returns_no_new_reason():
    st = _st(total=100, captcha=100, failed=100, paused=True)
    assert fp.lane_pause_reason(st, active_lane_count=5) == ""


def test_last_active_lane_is_never_paused():
    # 即使这条lane彻底失败，只要它是仅剩的一条活跃lane，也不能暂停，
    # 否则worker全部退出后任务队列无人消费，task_q.join() 会永久阻塞。
    total = fp.PRODUCT_LANE_FAILURE_MIN_SAMPLES + 10
    st = _st(total=total, captcha=0, failed=total)
    assert fp.lane_pause_reason(st, active_lane_count=1) == ""
    assert fp.lane_pause_reason(st, active_lane_count=0) == ""


def test_second_to_last_lane_can_still_be_paused():
    total = fp.PRODUCT_LANE_FAILURE_MIN_SAMPLES + 10
    st = _st(total=total, captcha=0, failed=total)
    assert fp.lane_pause_reason(st, active_lane_count=2) == "failure_rate"


def test_mixed_moderate_captcha_and_moderate_timeout_does_not_double_count():
    # captcha 和 failed 各自没到阈值，混合起来也不应该误触发（两者是独立判据，
    # 不是相加判定）。
    total = fp.PRODUCT_LANE_CAPTCHA_MIN_SAMPLES
    st = _st(total=total, captcha=total // 4, failed=total // 4)
    assert fp.lane_pause_reason(st, active_lane_count=5) == ""
