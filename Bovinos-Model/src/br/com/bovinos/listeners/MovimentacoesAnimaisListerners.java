package br.com.bovinos.listeners;

import java.util.Date;

import com.sankhya.util.TimeUtils;

import br.com.sankhya.jape.event.PersistenceEvent;
import br.com.sankhya.jape.event.PersistenceEventAdapter;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.auth.AuthenticationInfo;
import sun.security.util.Length;

public class MovimentacoesAnimaisListerners extends PersistenceEventAdapter {


	private static final long serialVersionUID = 1L;

	@Override
	public void beforeInsert(PersistenceEvent event) throws Exception {
		
		AuthenticationInfo auth = AuthenticationInfo.getCurrent();
		
		DynamicVO usu = (DynamicVO) auth.getUsuVO();
			
	 	DynamicVO newRegistro = (DynamicVO) event.getVo();
		
		newRegistro.setProperty("CODUSU", usu.asBigDecimal("CODUSU") );
		newRegistro.setProperty("DTALTER", TimeUtils.getNow());
		newRegistro.setProperty("STATUSMOV", "P");
		newRegistro.setProperty("DTINCLUSAO", TimeUtils.getNow());
	}
}
