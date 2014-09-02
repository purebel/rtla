package com.rtla.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class RTLARecordProcessorFactory implements IRecordProcessorFactory {
	
	/**
	 * Constructor
	 */
	public RTLARecordProcessorFactory() {
		super();
	}

	@Override
	public IRecordProcessor createProcessor() {
		// TODO Auto-generated method stub
		return new RTLARecordProcessor();
	}

}
