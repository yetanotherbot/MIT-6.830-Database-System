package simpledb;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

    private Type[] fieldTypes;
    private String[] fieldNames;
    private int size;

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
        int numFields1 = td1.numFields(), numFields2 = td2.numFields();
        Type[] comFieldTypes = new Type[numFields1 + numFields2];
        String[] comFieldNames = new String[numFields1 + numFields2];
        System.arraycopy(td1.fieldTypes, 0, comFieldTypes, 0, numFields1);
        System.arraycopy(td2.fieldTypes, 0, comFieldTypes, numFields1, numFields2);
        System.arraycopy(td1.fieldNames, 0, comFieldNames, 0, numFields1);
        System.arraycopy(td2.fieldNames, 0, comFieldNames, numFields1, numFields2);
        return new TupleDesc(comFieldTypes, comFieldNames);
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        fieldTypes = typeAr;
        fieldNames = fieldAr;
        for (Type type: fieldTypes)
            size += type.getLen();
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        fieldTypes = typeAr;
        fieldNames = new String[typeAr.length];
        for (Type type: fieldTypes)
            size += type.getLen();
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return fieldTypes.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= numFields())
            throw new NoSuchElementException();
        return fieldNames[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        if (name == null) throw new NoSuchElementException();
        int i = 0;
        for (String fieldName: fieldNames) {
            if (name.equals(fieldName)) return i;
            ++i;
        }
        throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= numFields())
            throw new NoSuchElementException();
        return fieldTypes[i];
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return size;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o == null) return false;
        if (!TupleDesc.class.isAssignableFrom(o.getClass()))
            return false;
        final TupleDesc other = (TupleDesc) o;
        if (getSize() != other.getSize() || numFields() != other.numFields())
            return false;
        for (int i = 0; i < numFields(); ++i)
            if (!fieldTypes[i].equals(other.fieldTypes[i]))
                return false;
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numFields(); ++i) {
            Type type = getType(i);
            String name = getFieldName(i);
            sb.append(type == Type.INT_TYPE ? "INT_TYPE" : "STRING_TYPE");
            sb.append('(');
            sb.append(name);
            if (i < numFields() - 1) sb.append("), ");
            else sb.append(")");
        }
        return sb.toString();
    }
}
