/*
    Implementação da operação binaria Union
    Os dados já precisam estar ordenados
    Registros com a mesma chave primária são concatenados
 */
package ibd2.query;

/**
 *
 * @author flan
 */
public class FrancielleUnion implements BinaryOperation {
    Operation op1;
    Operation op2;

    Tuple curTuple; // current Tuple operation 1
    Tuple curTuple2; // current Tuple operation 2
    Tuple nextTuple;

    public FrancielleUnion (Operation op1, Operation op2) throws Exception {
        super();
        this.op1 = op1;
        this.op2 = op2;
    }
    
    /*
        Prepara tudo para que se comece a fazer a junção (abre cursores, limpa variáveis, ...)
    */
    @Override
    public void open() throws Exception {
        op1.open();
        op2.open();
        this.curTuple = null;
        this.curTuple2 = null;
        this.nextTuple = null;
    }
    
    /*
        Recupera o próximo registro
    */
    @Override
    public Tuple next() throws Exception {
        if (nextTuple != null) { // se a nextTuple já foi setada, retorna ela
            Tuple next_ = nextTuple;
            nextTuple = null;
            return next_;
        }
        if (!op1.hasNext() && !op2.hasNext()) throw new Exception("No more data");
        if (curTuple == null) 
            if (op1.hasNext()) curTuple = op1.next(); // itera op1
        if (curTuple2 == null)
            if (op2.hasNext()) curTuple2 = op2.next(); // itera op2
        
        Tuple rec = new Tuple(); // enquanto há registros, há nextTuple
        while (true) {
            if (curTuple == null && curTuple2 != null) { // se somente op2 ainda possui registros
                curTuple2 = setTuple(rec, curTuple2);
                return rec;
            }
            else if (curTuple2 == null && curTuple != null) { // se somente op1 ainda possui registros
                curTuple = setTuple(rec, curTuple);
                return rec;
            } else { // se tuple1 e tuple2 nao sao nulas (op1 e op2 ainda possuem registros)
                if (curTuple.primaryKey == curTuple2.primaryKey) {
                    rec.primaryKey = curTuple.primaryKey;
                    rec.content = curTuple.content + " " + curTuple2.content;
                    curTuple = null;
                    curTuple2 = null;
                    return rec;
                }
                else if (curTuple.primaryKey < curTuple2.primaryKey) {
                    curTuple = setTuple(rec, curTuple);
                    return rec;
                }
                else {
                    curTuple2 = setTuple(rec, curTuple2);
                    return rec;
                }
            }
        }
    }

    @Override
    public boolean hasNext() throws Exception {
        if (!op1.hasNext() && !op2.hasNext()) return false;
        if (curTuple == null) 
            if (op1.hasNext()) curTuple = op1.next(); // itera op1
        if (curTuple2 == null)
            if (op2.hasNext()) curTuple2 = op2.next(); // itera op2
        
        nextTuple = new Tuple(); // enquanto há registros, há nextTuple
        while (true) {
            if (curTuple == null && curTuple2 != null) { // se somente op2 ainda possui registros
                curTuple2 = setTuple(nextTuple, curTuple2);
                return true;
            }
            else if (curTuple2 == null && curTuple != null) { // se somente op1 ainda possui registros
                curTuple = setTuple(nextTuple, curTuple);
                return true;
            } else { // se tuple1 e tuple2 nao sao nulas (op1 e op2 ainda possuem registros)
                if (curTuple.primaryKey == curTuple2.primaryKey) {
                    nextTuple.primaryKey = curTuple.primaryKey;
                    nextTuple.content = curTuple.content + " " + curTuple2.content;
                    curTuple = null;
                    curTuple2 = null;
                    return true;
                }
                else if (curTuple.primaryKey < curTuple2.primaryKey) {
                    curTuple = setTuple(nextTuple, curTuple);
                    return true;
                }
                else {
                    curTuple2 = setTuple(nextTuple, curTuple2);
                    return true;
                }
            }
        }
    }
    
    @Override
    public Operation getLeftOperation() {
        return op1;
    }

    @Override
    public Operation getRigthOperation() {
        return op2;
    }
    
    @Override
    public void close() throws Exception {
    }
    
    private Tuple setTuple (Tuple tuple, Tuple curTuple) {
        tuple.primaryKey = curTuple.primaryKey;
        tuple.content = curTuple.content;
        return null;
    }
}
