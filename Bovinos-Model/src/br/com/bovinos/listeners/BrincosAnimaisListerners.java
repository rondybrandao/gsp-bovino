package br.com.bovinos.listeners;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;

import com.sankhya.util.TimeUtils;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.PersistenceEventAdapter;
import br.com.sankhya.jape.util.FinderWrapper;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.auth.AuthenticationInfo;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import sun.security.util.Length;

public class BrincosAnimaisListerners extends PersistenceEventAdapter {


	private static final long serialVersionUID = 1L;

	@Override
	public void beforeInsert(PersistenceEvent event) throws Exception {
		
		AuthenticationInfo auth = AuthenticationInfo.getCurrent();
		
		DynamicVO usu = (DynamicVO) auth.getUsuVO();	
	 	DynamicVO vo = (DynamicVO) event.getVo();
		
	 	vo.setProperty("CODUSU", usu.asBigDecimal("CODUSU") );
	 	vo.setProperty("DTCAD", TimeUtils.getNow());
		
		BigDecimal IDANIMAL = vo.asBigDecimalOrZero("IDANIMAL");
		
        final EntityFacade dwfEntityFacade = EntityFacadeFactory.getDWFFacade();
        final FinderWrapper brincosAnimais = new FinderWrapper("BrincosAnimais", "this.IDANIMAL = ? AND this.ATIVO = 'S' ", new Object[]{IDANIMAL});
        final Collection<DynamicVO> brincosAnimaisList = (Collection<DynamicVO>) dwfEntityFacade.findByDynamicFinderAsVO(brincosAnimais);
        
        if (brincosAnimaisList.size() > 0) {
        	String NUBRINCO = "";
        	for(DynamicVO itembrincosAnimais : brincosAnimaisList){
        		NUBRINCO = itembrincosAnimais.asString("NUBRINCO");
        	}
            throw new Exception("<b>Este animal possui vinculo ativo com o brinco " + NUBRINCO + "! Não é possível vincula-lo novamente.</b>");
        }
	}	
	
}
