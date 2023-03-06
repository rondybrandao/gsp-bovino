package br.com.bovinos.listeners;

import java.math.BigDecimal;
import java.util.Collection;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.PersistenceEventAdapter;
import br.com.sankhya.jape.util.FinderWrapper;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class ItemMovimentacoesAnimalListerners extends PersistenceEventAdapter {


	private static final long serialVersionUID = 1L;

	@Override
	public void beforeUpdate(PersistenceEvent event) throws Exception {
		DynamicVO vo = (DynamicVO) event.getVo();
		
		BigDecimal IDMOV = vo.asBigDecimalOrZero("IDMOV");
		BigDecimal IDANIMAL = vo.asBigDecimalOrZero("IDANIMAL");
		
        final EntityFacade dwfEntityFacade = EntityFacadeFactory.getDWFFacade();
        final FinderWrapper itemMovAnimal = new FinderWrapper("ItemMovimentacoesAnimal", "this.IDMOV = ? AND this.IDANIMAL = ? ", new Object[]{IDMOV, IDANIMAL});
        final Collection<DynamicVO> itemMovAnimalList = (Collection<DynamicVO>) dwfEntityFacade.findByDynamicFinderAsVO(itemMovAnimal);
        
        if (itemMovAnimalList.size() > 0) {
            throw new Exception("<b>Este animal já está vinculado! Não é possível vincula-lo novamente.</b>");
        }
	}
	
	
}
