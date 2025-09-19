from mbst.bleusb_comm_toolkit import insert_and_retrieve_message_streams

if __name__ == "__main__":
    try:
        insert_and_retrieve_message_streams("Example Script test")
    except Exception as e:
        print(f"Failed to insert and retrieve message: {e}")
