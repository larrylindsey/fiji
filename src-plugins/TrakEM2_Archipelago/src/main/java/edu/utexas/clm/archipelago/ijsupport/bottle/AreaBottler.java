package edu.utexas.clm.archipelago.ijsupport.bottle;

import edu.utexas.clm.archipelago.network.MessageXC;
import edu.utexas.clm.archipelago.network.translation.Bottle;
import edu.utexas.clm.archipelago.network.translation.Bottler;

import java.awt.geom.Area;

/**
 *
 */
public class AreaBottler implements Bottler<Area>
{

    public boolean accepts(Object o)
    {
        return o instanceof Area;
    }

    public Bottle<Area> bottle(Object o, MessageXC xc)
    {
        return new AreaBottle((Area)o);
    }

    public boolean transfer()
    {
        return true;
    }
}
