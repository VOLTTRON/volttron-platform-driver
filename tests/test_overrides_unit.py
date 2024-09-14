# TODO old tests to either recreate or throw out

# PlatformDriverAgent.__bases__ = (AgentMock.imitate(Agent, Agent()), )
#
#
# @pytest.mark.parametrize("pattern, expected_device_override", [("campus/building1/*", 1),
#                                                                ("campus/building1/", 1),
#                                                                ("wrongcampus/building", 0)])
# def test_set_override_on_should_succeed(pattern, expected_device_override):
#
#         platform_driver_agent.set_override_on(pattern)
#
#         assert len(platform_driver_agent._override_patterns) == 1
#         assert len(platform_driver_agent._override_devices) == expected_device_override
#         platform_driver_agent.vip.config.set.assert_called_once()
#
#
# def test_set_override_on_should_succeed_on_definite_duration():
#     pattern = "campus/building1/*"
#     duration = 42.9
#     override_interval_events = {"campus/building1/*": None}
#
#     with pdriver(override_interval_events=override_interval_events) as platform_driver_agent:
#         platform_driver_agent.set_override_on(pattern, duration=duration)
#
#         assert len(platform_driver_agent._override_patterns) == 1
#         assert len(platform_driver_agent._override_devices) == 1
#         platform_driver_agent.vip.config.set.assert_not_called()
#
#
# def test_set_override_off_should_succeed():
#     patterns = {"foobar", "device1"}
#     override_interval_events = {"device1": None}
#     pattern = "foobar"
#
#     with pdriver(override_interval_events=override_interval_events,
#                  patterns=patterns) as platform_driver_agent:
#         override_patterns_count = len(platform_driver_agent._override_patterns)
#
#         platform_driver_agent.set_override_off(pattern)
#
#         assert len(platform_driver_agent._override_patterns) == override_patterns_count - 1
#         platform_driver_agent.vip.config.set.assert_called_once()
#
#
# def test_set_override_off_should_raise_override_error():
#     with pytest.raises(OverrideError):
#         with pdriver() as platform_driver_agent:
#             pattern = "foobar"
#             platform_driver_agent.set_override_off(pattern)

# def test_clear_overrides():
#     override_patterns = set("ffdfdsfd")
#
#     with pdriver(override_patterns=override_patterns) as platform_driver_agent:
#         platform_driver_agent.clear_overrides()
#
#         assert len(platform_driver_agent._override_interval_events) == 0
#         assert len(platform_driver_agent._override_devices) == 0
#         assert len(platform_driver_agent._override_patterns) == 0

# @contextlib.contextmanager
# def pdriver(override_patterns: set = set(),
#             override_interval_events: dict = {},
#             patterns: dict = None,
#             scalability_test: bool = None,
#             waiting_to_finish: set = None,
#             current_test_start: datetime = None):
#     driver_config = json.dumps({
#         "driver_scrape_interval": 0.05,
#         "publish_breadth_first_all": False,
#         "publish_depth_first": False,
#         "publish_breadth_first": False
#     })
#
#     if scalability_test:
#         platform_driver_agent = PlatformDriverAgent(driver_config,
#                                                     scalability_test=scalability_test)
#     else:
#         platform_driver_agent = PlatformDriverAgent(driver_config)
#
#     platform_driver_agent._override_patterns = override_patterns
#     platform_driver_agent.instances = {"campus/building1/": MockedInstance()}
#     platform_driver_agent.core.spawn_return_value = None
#     platform_driver_agent._override_interval_events = override_interval_events
#     platform_driver_agent._cancel_override_events_return_value = None
#     platform_driver_agent.vip.config.set.return_value = ""
#
#     if patterns is not None:
#         platform_driver_agent._override_patterns = patterns
#     if waiting_to_finish is not None:
#         platform_driver_agent.waiting_to_finish = waiting_to_finish
#     if current_test_start is not None:
#         platform_driver_agent.current_test_start = current_test_start
#
#     try:
#         yield platform_driver_agent
#     finally:
#         platform_driver_agent.vip.reset_mock()
#         platform_driver_agent._override_patterns.clear()
#
#
# class MockedInstance:
#
#     def revert_all(self):
#         pass
# ########
