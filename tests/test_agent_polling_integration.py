import json
import tempfile
from pathlib import Path
import pytest
from volttrontesting.platformwrapper import InstallAgentOptions
import re
import time

"""
This test uses the platform wrapper. It sets up a real instance, installs the platform driver agent,
fake driver, and listener agent. It then checks if the correct amount of publishes happened by counting
how many times it sees 'devices/singletestfake/multi' in the log. To avoid mistakes, it clears the log 
before counting. 
"""

@pytest.fixture(scope="module")
def driver_setup(volttron_instance):
    """Set up a volttron instance with a fake driver."""
    vi = volttron_instance

    # Install the fake driver library
    library_path = Path("/home/riley/DRIVERWORK/11rc1/volttron-lib-fake-driver").resolve()
    vi.install_library(library_path)

    # Create and store the main platform driver config
    main_config = {
        "allow_duplicate_remotes": False,
        "max_open_sockets": 5,
        "max_concurrent_publishes": 5,
        "scalability_test": False,
        "groups": {
            "default": {
                "frequency": 1,  # Polling frequency in seconds
                "points": ["*"]  # Poll all points
            }
        }
    }
    main_config_path = Path(tempfile.mktemp(suffix="_main_driver_config.json"))
    with main_config_path.open("w") as file:
        json.dump(main_config, file)

    # Store the main driver config in the config store
    vi.run_command(["vctl", "config", "store", "platform.driver", "config", str(main_config_path), "--json"])

    # Create and store a fake driver device config
    device_config = {
        "driver_config": {},
        "registry_config": "config://singletestfake.csv",
        "interval": 1,
        "timezone": "US/Pacific",
        "heart_beat_point": "Heartbeat",
        "driver_type": "fake",
        "active": True
    }
    device_config_path = Path(tempfile.mktemp(suffix="_driver_config.json"))
    with device_config_path.open("w") as file:
        json.dump(device_config, file)
    vi.run_command(["vctl", "config", "store", "platform.driver", "devices/singletestfake", str(device_config_path), "--json"])

    # Create and store a CSV registry config
    with tempfile.NamedTemporaryFile(mode='w', suffix=".csv", delete=False) as temp_csv:
        temp_csv.write(
            "Point Name,Volttron Point Name,Units,Units Details,Writable,Starting Value,Type,Notes\n"
            "TestPoint1,TestPoint1,PPM,1000.00 (default),TRUE,10,float,Test point 1\n"
        )
        csv_path = temp_csv.name

    vi.run_command(["vctl", "config", "store", "platform.driver", "singletestfake.csv", csv_path, "--csv"])

    # Install the listener
    vi.run_command(["vctl", "install", "volttron-listener", "--start"])

    # Install the platform driver agent but don't start it yet
    agent_dir = str(Path(__file__).parent.parent.resolve())
    agent_uuid = vi.install_agent(
        agent_dir=agent_dir,
        install_options=InstallAgentOptions(
            start=False,
            vip_identity="platform.driver"
        )
    )
    assert agent_uuid is not None

    return vi, agent_uuid, "singletestfake"

def test_interval_publishes(driver_setup):
    vi, agent_uuid, device_name = driver_setup

    # Path to the volttron.log file
    log_file = Path(vi.volttron_home).parent / 'volttron.log'

    # Start the platform driver agent
    vi.start_agent(agent_uuid)
    print("Platform driver agent started.")

    # Wait for the agent to start and initialize fully
    time.sleep(5)

    # Clear (truncate) the log file after the agent has started
    with log_file.open('w') as f:
        f.truncate(0)
    print("Log file truncated after agent startup.")

    # Wait for the polling interval
    wait_time = 7  # seconds
    frequency = 1  # polling frequency from your driver config
    expected_publishes = wait_time // frequency

    print(f"Waiting for {wait_time} seconds...")
    time.sleep(wait_time)

    # Read the log contents and count the publishes
    with log_file.open('r') as f:
        log_contents = f.read()

        print("#"*50)
        print("#" * 50)
        print(log_contents)
        print("#" * 50)
        print("#" * 50)

    # Use a specific pattern to match only the driver's publish log entries
    publish_pattern = re.compile(
        rf"""SERVER DEBUG: publishing: devices/{device_name}/multi"""
    )
    publish_count = len(publish_pattern.findall(log_contents))
    print(f"Publish count in {wait_time} seconds: {publish_count}")

    # Assert that the number of publishes meets expectations
    assert publish_count >= expected_publishes, (
        f"Expected at least {expected_publishes} publishes, but got {publish_count}"
    )