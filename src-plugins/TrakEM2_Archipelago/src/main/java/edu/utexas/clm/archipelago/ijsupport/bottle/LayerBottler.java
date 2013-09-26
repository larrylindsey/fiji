package edu.utexas.clm.archipelago.ijsupport.bottle;

import edu.utexas.clm.archipelago.FijiArchipelago;
import edu.utexas.clm.archipelago.network.translation.Bottle;
import edu.utexas.clm.archipelago.network.translation.Bottler;
import ini.trakem2.display.Layer;

/**
 *
 */
public class LayerBottler implements Bottler<Layer>
{


    public boolean accepts(Object o)
    {
        return o instanceof Layer;
    }

    public Bottle<Layer> bottle(Object o)
    {
        return new LayerBottle((Layer)o);
    }

    public boolean transfer()
    {
        return false;
    }
}
