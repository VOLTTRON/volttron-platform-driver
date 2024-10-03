# TODO old tests to recreate or throw out

# def test_scrape_starting_should_return_none_on_false_scalability_test():
#     topic = "mytopic/foobar"
#
#     with pdriver() as platform_driver_agent:
#         assert platform_driver_agent.scrape_starting(topic) is None
#
#
# def test_scrape_starting_should_start_new_measurement_on_true_scalability_test():
#     topic = "mytopic/foobar"
#
#     with pdriver(scalability_test=True) as platform_driver_agent:
#         platform_driver_agent.scrape_starting(topic)
#
#         assert platform_driver_agent.current_test_start < datetime.now()
#         # This should equal the size of the agent's instances
#         assert len(platform_driver_agent.waiting_to_finish) == 1
#
#
# def test_scrape_ending_should_return_none_on_false_scalability_test():
#     topic = "mytopic/foobar"
#
#     with pdriver() as platform_driver_agent:
#         assert platform_driver_agent.scrape_ending(topic) is None
#
#
# def test_scrape_ending_should_increase_test_results_iterations():
#     waiting_to_finish = set()
#     waiting_to_finish.add("mytopic/foobar")
#     topic = "mytopic/foobar"
#
#     with pdriver(scalability_test=True,
#                  waiting_to_finish=waiting_to_finish,
#                  current_test_start=datetime.now()) as platform_driver_agent:
#         platform_driver_agent.scrape_ending(topic)
#
#         assert len(platform_driver_agent.test_results) > 0
#         assert platform_driver_agent.test_iterations > 0
