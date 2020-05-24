package ibd2.query;

import ibd2.Block;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author flan
 */

public class FrancielleBlockNestedLoopJoin implements BinaryOperation {
    Operation op1;
    Operation op2;
    Tuple curTuple;
    Tuple nextTuple;
    private List<Tuple> joinBuffer; // lista com todas as tuplas de um bloco

    public FrancielleBlockNestedLoopJoin(Operation op1, Operation op2) {
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
        this.joinBuffer = new ArrayList<>();
        this.curTuple = null;
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

        if (joinBuffer.size() <= 0) { // se o buffer está vazio
            fillBuffer(); // enche o buffer
        }
        while (op1.hasNext() || joinBuffer.size() > 0) {
            if (joinBuffer.size() > 0) { // se tem algum registro da 1ª operação
                // verificar todos registros da op2 e comparar com op1
                while (op2.hasNext()) { // percorre a tabela 2
                    curTuple = op2.next(); // atualiza a tupla atual a ser comparada
                    for (Tuple t : joinBuffer) { // para cada elemento de op2, percorre o buffer
                        if (curTuple.primaryKey == t.primaryKey) { // se as PK forem iguais, elemento encontrado
                            Tuple rec = new Tuple();
                            rec.primaryKey = t.primaryKey;
                            rec.content = t.content + " " + curTuple.content;
                            return rec;
                        }
                    }
                }
            }
            /* op2 foi totalmente scaneada */
            if (!op2.hasNext()) { // por garantia testa se op2 ñ possui mais registros
                joinBuffer.clear(); // reseta o buffer
                fillBuffer(); // preenche o buffer com os demais dados
            }
        }

        throw new Exception("No record after this point");
    }

    /*
        Verifica se há mais elementos a recuperar
    */
    @Override
    public boolean hasNext() throws Exception {
        if (nextTuple != null) { // se ja tem o proximo registro setado ainda há registros na tabela
            return true;
        }
        
        if (joinBuffer.size() <= 0) { // se o buffer tá vazio
            fillBuffer(); // enche o buffer
        }
        while (op1.hasNext() || joinBuffer.size() > 0) {
            if (joinBuffer.size() > 0) { // se tem algum registro da 1ª operação
                // verificar todos registros da op2 e comparar com op1
                while (op2.hasNext()) {
                    curTuple = op2.next();
                    for (Tuple t : joinBuffer) { // para cada elemento de op2, percorre o buffer
                        if (curTuple.primaryKey == t.primaryKey) {
                            nextTuple = new Tuple();
                            nextTuple.primaryKey = t.primaryKey;
                            nextTuple.content = t.content + " " + curTuple.content;
                            return true;
                        }
                    }
                }
            }
            /* op2 foi totalmente scaneada */
            if (!op2.hasNext()) {
                joinBuffer.clear();
                fillBuffer();
                op2.open();
            }
        }
        return false; // todos os loops foram concluídos, logo n há mais registros
    }
    
    /* Preenche o buffer com tuplas da operação 1, com tamanho equivalente a Block.RECORDS_AMOUNT*/
    private void fillBuffer () throws Exception {
        while (op1.hasNext() && joinBuffer.size() < Block.RECORDS_AMOUNT) {
            Tuple t = op1.next();
            joinBuffer.add(t);
        }
    }

    @Override
    public void close() throws Exception {
    }
    
    @Override
    public Operation getLeftOperation() {
        return op1;
    }

    @Override
    public Operation getRigthOperation() {
        return op2;
    }
}
