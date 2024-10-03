import re
import logging
import time

MESSAGE_TIMEOUT = 60

def chunKing(addr, message_fragments, message):
    try:
        match = re.search(r'CISE_\w+ (\d+) (\d+) (\d+)', message)
        if match:
            unique_id = match.group(1)
            total_chunks = int(match.group(2))
            current_chunk = int(match.group(3))
            
            if unique_id not in message_fragments:
                message_fragments[unique_id] = {'total': total_chunks, 'received': [], 'complete': False, 'timestamp': time.time(), 'ip': addr}

            message_fragments[unique_id]['received'].append((current_chunk, message))
            message_fragments[unique_id]['timestamp'] = time.time()

            if len(message_fragments[unique_id]['received']) == message_fragments[unique_id]['total']:
                message_fragments[unique_id]['complete'] = True

            if message_fragments[unique_id]['complete']:
                full_message = ''.join(msg for _, msg in sorted(message_fragments[unique_id]['received']))
                logging.info(f"Full message joined: {unique_id}")
                del message_fragments[unique_id]
                return full_message
            else:
                current_time = time.time()
                for uid, data in list(message_fragments.items()):
                    if not data['complete']:
                        if current_time - data['timestamp'] > MESSAGE_TIMEOUT:
                            full_message = ''.join(msg for _, msg in sorted(data['received']))
                            received_chunks = len(data['received'])
                            logging.warning(f"{data['ip']} {uid} Fragment waiting over {MESSAGE_TIMEOUT}, received {received_chunks}/{data['total']} chunks, last updated {time.ctime(data['timestamp'])}")
                            del message_fragments[uid]
                            return full_message
        else:
            logging.warning(f"No regex match on chunKing {message}")

    except Exception as e:
        logging.error(f"Error in chunKing: {e}")
    return None
