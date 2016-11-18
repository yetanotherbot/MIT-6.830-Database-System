package simpledb;

/**
 * Created by musteryu on 2016/11/9.
 */
class Node<E> {
    private E element;
    private Node<E> last;
    private Node<E> next;

    public Node(E e) {
        element = e;
        last = next = null;
    }

    public Node(E e, Node<E> last, Node<E> next) {
        element = e;
        this.last = last;
        this.next = next;
    }

    public Node<E> insertAfterBy(Node<E> n) {
        if (n == null) return null;
        n.last = this;
        n.next = this.next;
        if (this.next != null)
            this.next.last = n;
        this.next = n;
        return n;
    }

    public Node<E> insertBeforeBy(Node<E> n) {
        if (n == null) return null;
        n.next = this;
        n.last = this.last;
        if (this.last != null)
            this.last.next = n;
        this.last = n;
        return n;
    }

    public Node<E> removeAfter() {
        if (next != null)
            return next.unlink();
        else return null;
    }

    public Node<E> removeBefore() {
        if (last != null)
            return last.unlink();
        else return null;
    }

    public Node<E> unlink() {
        if (this.last != null)
            this.last.next = this.next;
        if (this.next != null)
            this.next.last = this.last;
        this.last = this.next = null;
        return this;
    }

    public E unwrap() {
        return element;
    }
}
