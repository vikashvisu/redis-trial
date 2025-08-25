import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class MultiHandler {

	private boolean state;
	private Queue<List<String>> queue;

	public void init() {
		queue = new LinkedList<>();
		state = true;
	}

	public boolean isOn() {
		return state;
	}

	public void clear() {
		state = false;
		queue.clear();
		queue = null;
	}

	public void add(List<String> command) {
		queue.offer(command);
	}

	public boolean isEmpty() {
		return queue == null || queue.isEmpty();
	}

	public List<String> getNext() {
		return queue.poll();
	}

	public int getSize() {
		return queue.size();
	}
}
