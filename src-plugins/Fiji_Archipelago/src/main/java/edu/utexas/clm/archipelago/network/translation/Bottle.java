package edu.utexas.clm.archipelago.network.translation;

import java.io.Serializable;

/**
 *
 */
public interface Bottle<A> extends Serializable
{

    public A unBottle();

}
