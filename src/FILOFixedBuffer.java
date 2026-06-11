import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FILOFixedBuffer<E> {
    private final ArrayList<E> backingList = new ArrayList<>();
    private final int maxSize;

    public FILOFixedBuffer(int maxSize) {
        this.maxSize = maxSize;
    }

    public void push(E item) {
        backingList.addFirst(item);
        if (backingList.size() >= maxSize) {
            backingList.removeLast();
        }
    }

    public E get(int index) {
        return backingList.get(index);
    }

    public E remove(int index) {
       return backingList.remove(index);
    }

    public void clear() {
        backingList.clear();
    }

    public int size() {
        return backingList.size();
    }

}
