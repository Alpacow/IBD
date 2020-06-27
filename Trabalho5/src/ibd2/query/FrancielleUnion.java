/*
    Implementação da operação binaria Union
    Os dados já precisam estar ordenados
    Registros com a mesma chave primária são concatenados

OBS: o número de blocos carregados durante a união deve 
ser proporcional ao número total de blocos
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
        throw new Exception("No more data");
    }

    @Override
    public boolean hasNext() throws Exception {
        if (!op1.hasNext() && !op2.hasNext())
            return false;
        if (curTuple == null) 
            if (op1.hasNext())
                curTuple = op1.next();

        if (curTuple2 == null)
            if (op2.hasNext())
                curTuple2 = op2.next();
        
        nextTuple = new Tuple();
        while (true) {
            if (curTuple == null && curTuple2 != null) {
                nextTuple.primaryKey = curTuple2.primaryKey;
                nextTuple.content = curTuple2.content;
                curTuple2 = null;
                return true;
            }
            else if (curTuple2 == null && curTuple != null) {
                nextTuple.primaryKey = curTuple.primaryKey;
                nextTuple.content = curTuple.content;
                curTuple = null;
                return true;
            } else {
                if (curTuple.primaryKey == curTuple2.primaryKey) {
                    nextTuple.primaryKey = curTuple.primaryKey;
                    nextTuple.content = curTuple.content + " " + curTuple2.content;
                    curTuple = null;
                    curTuple2 = null;
                    return true;
                }
                else if (curTuple.primaryKey < curTuple2.primaryKey) {
                    nextTuple.primaryKey = curTuple.primaryKey;
                    nextTuple.content = curTuple.content;
                    curTuple = null;
                    return true;
                }
                else {
                    nextTuple.primaryKey = curTuple2.primaryKey;
                    nextTuple.content = curTuple2.content;
                    curTuple2 = null;
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
    
}
